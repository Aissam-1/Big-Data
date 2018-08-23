package com.iqvia.rdf.dp.parser.uk.transaction

import org.apache.spark.sql.functions.{lit, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}


class PatientParser extends BaseParser {

  def map(df: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._

    val columnNames = Seq("AUDITFLAG", "AUDITSEQ", "AUDITED", "OPERAT_ID", "SYSDATE", "SYSTIME", "ENTITY_ID", "RI_CODE", "DHA", "FHSA", "SEX", "DOB", "BIRTHPLACE", "MARRIED", "HOUSE_RANK", "REG_STATUS", "ACCEPTANCE", "PROVISIONA", "APPLIED", "ACCEPT", "REG_GP", "USUAL_GP", "PREV_FHSA", "VAMP_OLDID", "DISPENSING", "PRESCRIBE", "OUT_DATE", "TRAN_REASO", "CAPP_SUPP", "SOURCE_REG", "CHSREG", "CHSDR", "CHSDATE", "FAMILY_NO")
    val dfRen: DataFrame = df.toDF(columnNames: _*)

    // No need to add columns to the schema and insert null (done automatically)
    //val columnsToAdd = Seq("SUB_ID", "MORTALITY_DATE", "WEIGHT", "HEIGHT", "INTEGRATED_CARE", "INTEGRATED_CARE_TYPE", "INSURANCE_TYPE", "SCALE_CHARGES", "HEALTH_INSURANCE", "TRA_ID_ORI", "REJECT_COUNT", "BMI", "BSA", "ETHNICITY_ID", "EXITUS", "INFORM_CONSENT_SIGNED", "INVESTIGATOR", "LOAD_ID", "REASON_OF_EXITUS", "REGISTER_ID", "SYSTEM_CREATION_DATE", "SYSTEM_UPDATE_DATE")

    val dfCorrectSchema = dfRen
      //.select(col("*") +: (columnsToAdd.map(c => lit(null).cast(StringType).as(c))): _*)
      .withColumn("TRA_ID", lit(500).cast("decimal(38,10)"))
      .withColumn("SUPP_ID", lit(13).cast("decimal(38,10)"))
      .withColumn("PRAC_ID", lit("12345"))
      .withColumn("BIRTH_YEAR", substring($"DOB", 0, 4))
      .withColumn("BIRTH_MONTH", substring($"DOB", 5, 2))
      .withColumnRenamed("SEX", "GEN_ID")
      .withColumnRenamed("SYSDATE", "SRC_SYSDATE")
      .withColumnRenamed("SYSTIME", "SRC_SYSTIME")
      .withColumnRenamed("ENTITY_ID", "PAT_ID")
      .withColumnRenamed("MARRIED", "PAT_MAR_ID")
      .withColumnRenamed("ACCEPT", "REGISTRATION_DATE")
      .withColumnRenamed("OUT_DATE", "REGISTRATION_OUT_DATE")
      .withColumnRenamed("TRAN_REASO", "PAT_STA_ID")
      .drop("DOB")

    dfCorrectSchema.printSchema()

    dfCorrectSchema
  }

}
