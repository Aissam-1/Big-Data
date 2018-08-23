package sparkworkshop

import org.apache.spark.sql.{DataFrame, SparkSession}

trait FileDataReader {

  def readData(spark: SparkSession, fileName: String, fileFormat: String): DataFrame = {
    spark
      .read
      .format(fileFormat)
      .option("header", true)
      .option("inferSchema", "true")
      .load(fileName)
  }
}
