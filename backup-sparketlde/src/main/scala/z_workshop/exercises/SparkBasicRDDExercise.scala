package sparkworkshop.exercises

import org.apache.spark.{SparkConf, SparkContext}

/**
  * EXERCISE.
  * Define an array of ints.
  * Then use parallelize method to create RDD.
  * Then println values in RDD and additionally
  * println values in every partition of RDD.
  */
object SparkBasicRDDExercise {

  def main(args: Array[String]): Unit = {
    println("SparkBasicRDDExercise started.")

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkBasicRDDExercise")

    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO:
//    val data = Array(1, 2, 3, 4, 5, 6, 7)
//    val distData = sc.parallelize(data)
//
//    distData.foreach(println)
//    distData.foreachPartition(iter => println(s"${iter.mkString(" ")}"))

    sc stop()
    println("SparkBasicRDDExercise stopped.")
  }
}
