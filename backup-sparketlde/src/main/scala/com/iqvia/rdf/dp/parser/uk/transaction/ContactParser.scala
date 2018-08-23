package com.iqvia.rdf.dp.parser.uk.transaction

import org.apache.spark.sql.functions.{lit, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}


class ContactParser extends BaseParser {

  def map(df: DataFrame, spark: SparkSession): DataFrame = {

    val columnNames = Seq("AUDITFLAG", "AUDITSEQ", "AUDITED", "OPERAT_ID", "SYSDATE", "SYSTIME", "ENTITY_TY", "ENTITY_ID", "MASTER_ID", "CLINICIAN", "EVENTDATE", "CATEGORY", "DURATION")
    val dfRen: DataFrame = df.toDF(columnNames: _*)

    val dfCorrectSchema = dfRen
      .withColumn("TRA_ID", lit(500).cast("decimal(38,10)"))
      .withColumn("SUPP_ID", lit(13).cast("decimal(38,10)"))
      .withColumn("PRAC_ID", lit("12345"))
      .withColumnRenamed("SYSDATE", "SRC_SYSDATE")
      .withColumnRenamed("SYSTIME", "SRC_SYSTIME")
      .withColumnRenamed("MASTER_ID", "PAT_ID")
      .withColumnRenamed("CLINICIAN", "DOC_ID")
      .withColumnRenamed("EVENTDATE", "CON_DATE")
      .withColumnRenamed("CATEGORY", "CON_TYP_ID")
      .withColumnRenamed("DURATION", "CON_DURATION")

    dfCorrectSchema
  }

}
