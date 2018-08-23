package z_myapp

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql._


object ETL_ThSuppProd {
  def main(args: Array[String]) {
    /**
      * INITIALIZATION & CONFIG
      * PS: A SORTIR D'ICI
      */
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[*]")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val od_prescription_tmp = "RWE_OD.OD_PRESCRIPTION_TMP"
    val th_supp_prod = "RWE_COMMON.TH_SUPP_PROD"

    val conf = ConfigFactory.load("application.properties")
    val stUrl = conf.getString("st.url")
    val stUser = conf.getString("st.username")
    val stPassword = conf.getString("st.password")
    val stConnectionProperties = new Properties()
    stConnectionProperties.put("user", stUser)
    stConnectionProperties.put("password", stPassword)
    val trUrl = conf.getString("tr.url")
    val trUser = conf.getString("tr.username")
    val trPassword = conf.getString("tr.password")
    val trConnectionProperties = new Properties()
    trConnectionProperties.put("user", trUser)
    trConnectionProperties.put("password", trPassword)

    spark.udf.register("NORM", (text: Any) => norm(text))

    /**
      * READ DataFrame FROM STAGING DATABASE
      * IMPORTANT : We select only interesting columns
      */
    val odPrescriptionDF = spark.read.jdbc(stUrl, "(select distinct DRUG_ADDITIONAL_INFO_1, DRUG_ADDITIONAL_INFO_2, DRUG_ADDITIONAL_INFO_3 from " + od_prescription_tmp + " ) bar", stConnectionProperties)

    /**
      * READ DataFrame FROM TRANSACTIONAL DATABASE,
      * TAKE ONLY 3 COLUMNS OF LOOKUP
      */
    val thSuppProdDF = spark.read.jdbc(trUrl, "(select SUPP_PROD_ID, SRC_PROD_LNG_TXT, SRC_PACK_SIZE, SRC_PROD_CD from " + th_supp_prod + ") foo", trConnectionProperties)

    odPrescriptionDF.createOrReplaceTempView("odprescription")
    thSuppProdDF.createOrReplaceTempView("thsuppprod")

    /**
      * LOOKUP: Products to be inserted into BO database
      */
    val prodToInsert: DataFrame = spark.sql("select SUPP_PROD_ID, SRC_PROD_LNG_TXT, SRC_PACK_SIZE, SRC_PROD_CD, " +
      "DRUG_ADDITIONAL_INFO_1, NORM(DRUG_ADDITIONAL_INFO_2) DRUG_ADDITIONAL_INFO_2, DRUG_ADDITIONAL_INFO_3 from odprescription " +
      "left outer join thsuppprod " +
      "on (NVL(NORM(NVL(DRUG_ADDITIONAL_INFO_2, '#')),'#')=NVL(SRC_PROD_LNG_TXT,'#') " +
      "and NVL(DRUG_ADDITIONAL_INFO_3,'#')=NVL(SRC_PACK_SIZE,'#') " +
      "and NVL(DRUG_ADDITIONAL_INFO_1,'#')=NVL(SRC_PROD_CD,'#'))")
      .where("SUPP_PROD_ID is null")


    import org.apache.spark.sql.functions._

    val colsToRemove = Seq("SUPP_PROD_ID", "SRC_PROD_LNG_TXT", "SRC_PACK_SIZE", "SRC_PROD_CD", "TRA_ID", "SUPP_ID", "PRAC_ID", "PAT_ID", "DOC_ID", "CON_NUM", "CON_DATE", "PRD_ID", "BNF_ID", "DIA_ID", "DIA_TEXT_ACUTE", "DIA_TEXT_REPEAT", "ICD10_ACUTE", "ICD10_REPEAT", "DIA_CERTAINTY", "DIA_SIDE_LOCALIZATION", "DIA_EXPLANATION", "DIA_DEGREE", "IS_LONG_PERIOD_DISEASE", "RENEWAL_NUMBER", "LPD_START_DATE", "IN_PREVENTION", "IS_PRIVATE", "TTY_ID", "PRE_SPE_ID", "SUBSTITUT_ID", "SUB_PRD_ID", "DIA_TYPE_ID", "RANK", "DOSAGE", "MIN_DOSAGE", "MAX_DOSAGE", "MIN_PER_PERIOD", "MAX_PER_PERIOD", "PER_ID", "MIN_DURATION", "MAX_DURATION", "PACK_NUMBER", "PACKAGE_SIZE", "TRT_DURATION_TABS", "IS_LONG_PERIOD", "PATIENT_MORTALITY", "PRESCRIPTION_CODE", "ADDITIONAL_INFO", "DRUG_ABBR", "DRUG_ADDITIONAL_INFO_5", "DRUG_ADDITIONAL_INFO_4", "DRUG_FLAG", "PRE_DRUG_TYPE_ID", "TIME", "TRA_ID_ORI", "REJECT_COUNT", "CODIGOTRA_PROTOCLO", "COMPASSIONATE_USE", "DISEASE_FREE_SURVIVAL_DAYS", "DI_F_SURVIVAL_CENSORED_DATA", "DOSAGE_CHANGE", "DOSAGE_CHANGE_REASON", "IS_CLINICAL_TRIAL", "IS_PROTECTED", "LOAD_ID", "MAX_CYCLES", "PLAN_CYCLES", "PRE_END_DATE", "PRE_END_REASON", "PROGRESSION_FREE_SURVIVAL_DAYS", "PR_F_SURVIVAL_CENSORED_DATA", "REGISTER_ID", "SYSTEM_CREATION_DATE", "SYSTEM_UPDATE_DATE", "VACCINE_TXT")

    val prodToInsertWithCorrectSchema2 = prodToInsert
      .select(prodToInsert.columns.filter(colName => !colsToRemove.contains(colName)).map(colName => new Column(colName)): _*)
      .withColumn("TRA_ID", lit(500).cast("decimal(38,10)"))
      .withColumn("VERSION", lit(1).cast("decimal(38,10)"))
      .withColumn("CTRY_CD", lit("DE"))
      .withColumn("DATA_PROVIDER_CD", lit("COMPUGROUP"))
      .withColumnRenamed("DRUG_ADDITIONAL_INFO_3", "SRC_PACK_SIZE")
      .withColumnRenamed("DRUG_ADDITIONAL_INFO_2", "SRC_PROD_LNG_TXT")
      .withColumnRenamed("DRUG_ADDITIONAL_INFO_1", "SRC_PROD_CD")
      .withColumn("VALUE_4", lit(null).cast(StringType))
      .withColumn("VALUE_5", lit(null).cast(StringType))
      .withColumn("ISRTED_DT", unix_timestamp(lit("19/04/18"), "dd/MM/yy").cast(TimestampType))
      .withColumn("ISRTED_BY", lit("RDF_SPARK_ETL"))


    import spark.implicits._

    /**
      * Return all the elements of the dataframe as an array
      * to the driver program to manage sequences
      */
    val sequence: Int = thSuppProdDF.withColumn("SUPP_PROD_ID", 'SUPP_PROD_ID.cast("Int")).agg(max("SUPP_PROD_ID")).first.getInt(0) + 1
    val schema = new StructType(Array(new StructField("SUPP_PROD_ID", LongType, false)) ++ prodToInsertWithCorrectSchema2.schema.fields)
    val index = spark.createDataFrame(prodToInsertWithCorrectSchema2.rdd.zipWithIndex().map(r => Row.fromSeq(Seq(r._2 + sequence) ++ r._1.toSeq)), schema)


    val result = index.dropDuplicates("CTRY_CD", "DATA_PROVIDER_CD", "SRC_PROD_LNG_TXT", "SRC_PACK_SIZE", "SRC_PROD_CD", "VALUE_4", "VALUE_5");
    result.persist()
    result.write.mode(SaveMode.Append).jdbc(trUrl, th_supp_prod, trConnectionProperties)

/*
    val count: Int = result.count().toInt
    for (i <- sequence to sequence + count) {
      try {
        result.where("SUPP_PROD_ID = " + i)
          .write.mode(SaveMode.Append).jdbc(trUrl, th_supp_prod, trConnectionProperties)
      } catch {
          case foo: Exception =>
            println("Error="+foo)
            result.where("SUPP_PROD_ID = " + i).show(10, false)
            System.exit(0)
      } finally {
        println("END")
      }
    }
*/

  }

  def norm(text: Any): String = {
    val asciiControlChars: String = "([\\x00-\\x1F])"
    val asciiPrintableChars: String = "([\\x22\\x27\\x5E\\x60\\x7F])"
    val asciiExtendedChars: String = "([\\x82\\x84\\x86\\x87\\x88\\x8B\\x8D\\x8F\\x90\\x91\\x92\\" + "x93\\x94\\x95\\x96\\x97\\x98\\x9B\\x9D\\xA0\\xA8\\xAB\\xAC\\xAD\\xAF\\xB4\\xB6\\xB7\\xB8\\xBA\\xBB])"

    text match {
      case s: String => s.replaceAll(asciiControlChars + "|" + asciiPrintableChars + "|" + asciiExtendedChars, "")
        .replaceAll(" {2,}", " ")
        .toUpperCase
        .trim
      case _ => ""
    }
  }

}
