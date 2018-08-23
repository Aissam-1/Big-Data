package sparkworkshop.examples

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSQLExample {

  case class Person(id: Long, name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSQLExample")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    try {
      val peopleDs: Dataset[Person] =
        spark createDataset (
          Seq(
            Person(1, "Bari", 10),
            Person(2, "Artur", 11),
            Person(3, "Robert", 12),
            Person(4, "Kamil", 13)))

      peopleDs.show()
    } finally {
      spark stop()
    }
  }
}
