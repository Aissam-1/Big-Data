package sparkworkshop

import org.apache.spark.sql.SparkSession

trait JobRunner {

  def run(args : Array[String], spark: SparkSession) : Unit
}
