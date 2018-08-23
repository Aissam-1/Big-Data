package sparkworkshop.exercises

import org.apache.spark.{SparkConf, SparkContext}

/**
  * EXERCISE.
  * Please count number of occurrences of different words in US constitution.
  * Please sort the results from greater to lower number of occurrences.
  * Save the result to given destFilePath.
  *
  */
object WordsCounterExercise {

  val srcFilePath = "test-data/const.txt"
  val destFilePath = "test-data/const_output"

  def main(args: Array[String]): Unit = {
    println("WordCounter started.")

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkContextExample")

    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO
//    val result =
//      sc.textFile(srcFilePath)
//        .flatMap(line => line.split("\\W+"))
//        .map(word => (word, 1))
//        .reduceByKey((count1, count2) => count1 + count2)
//        .sortBy(_._2)
//
//    result.saveAsTextFile(destFilePath)

    sc stop()
    println("WordCounter ended.")
  }

}
