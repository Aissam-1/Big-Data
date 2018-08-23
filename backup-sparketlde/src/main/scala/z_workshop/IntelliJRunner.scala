package sparkworkshop

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object IntelliJRunner {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("FromIntelliJRunnerApp")

    val spark: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate
    val args = Array("sparkworkshop.examples.UDFFunctionExample")

    SparkRunner run(args, spark)
  }
}
