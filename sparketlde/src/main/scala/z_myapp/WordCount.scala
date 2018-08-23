package z_myapp

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("UK Parser").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile("test-data/PATIENT.csv")
    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    count.saveAsTextFile("outfile")
    System.out.println("OK");
  }
}
