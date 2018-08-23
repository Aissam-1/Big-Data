package com.iqvia.rdf.dp.parser.uk.transaction

import java.io.{FileNotFoundException, IOException}
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}


abstract class BaseParser {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def read(spark: SparkSession, CSVFilePath: String): DataFrame = {
    try {
      spark.sql("SELECT * FROM csv.`" + CSVFilePath + "`")
    } catch {
      case e: Exception => logger.error(e.toString)
        null
    }
  }

  def map(df: DataFrame, spark: SparkSession): DataFrame


  def write(df: DataFrame, tableName: String, connectionProperties: Properties): Unit = {
    df.write.mode(SaveMode.Append).jdbc(connectionProperties.getProperty("url"), tableName, connectionProperties)
  }

  def clean(): Unit = {
    //
  }
}


