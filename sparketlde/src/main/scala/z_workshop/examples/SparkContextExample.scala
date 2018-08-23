package sparkworkshop.examples

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextExample {

  def main(args: Array[String]): Unit = {
    println("SparkContextExample started.")

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkContextExample")

    val sc: SparkContext = new SparkContext(sparkConf)

    val data = Array(1, 2, 3, 4, 5, 6, 7)
    val distData = sc.parallelize(data)

    // some logic here

    sc stop()
    println("SparkContextExample stopped.")
  }
}
