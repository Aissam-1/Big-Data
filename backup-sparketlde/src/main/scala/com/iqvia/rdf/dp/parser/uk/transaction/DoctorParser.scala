package com.iqvia.rdf.dp.parser.uk.transaction

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}


class DoctorParser extends BaseParser {

  def map(df: DataFrame, spark: SparkSession): DataFrame = {

    val columnNames = Seq("AUDITFLAG", "AUDITSEQ", "AUDITED", "OPERAT_ID", "SYSDATE", "SYSTIME", "ENTITY_ID", "INACTIVE", "SEX", "ROLE", "PPA_NO")
    val dfRen: DataFrame = df.toDF(columnNames: _*)

    val dfCorrectSchema = dfRen
      .withColumn("TRA_ID", lit(500).cast("decimal(38,10)"))
      .withColumn("SUPP_ID", lit(13).cast("decimal(38,10)"))
      .withColumn("PRAC_ID", lit("12345"))
      .withColumnRenamed("SYSDATE", "SRC_SYSDATE")
      .withColumnRenamed("SYSTIME", "SRC_SYSTIME")
      .withColumnRenamed("ENTITY_ID", "DOC_ID")
      .withColumnRenamed("SEX", "DOC_GEN_ID")
      .withColumnRenamed("ROLE", "ROL_ID")

    dfCorrectSchema
  }

}
