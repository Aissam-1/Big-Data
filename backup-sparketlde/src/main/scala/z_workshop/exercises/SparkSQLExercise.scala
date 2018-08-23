package sparkworkshop.exercises

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * EXERCISE.
  * On peopleDS you should filter people with age >= 12 and show only their names using Scala API.
  * Then register temporary view with people with age > 12 and show this data using SQL API
  */
object SparkSQLExercise {

  case class Person(id: Long, name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSQLExample")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    try {
      val peopleDs : Dataset[Person]=
        spark createDataset (
          Seq(
            Person(1, "Bari", 10),
            Person(2, "Artur", 11),
            Person(3, "Robert", 12),
            Person(4, "Kamil", 13)))

      peopleDs.show()

      // TODO
//      peopleDs
//        .where($"age" >= 12)
//        .select($"name")
//        .show()
//
//      peopleDs
//        .where($"age" > 12)
//        .createOrReplaceTempView("peopleGreaterThan12")
//      val filteredPeople = spark.sql("select * from peopleGreaterThan12")
//
//      filteredPeople.show()

    } finally {
      spark stop()
    }

    println("SparkSQLExample stop.")
  }
}
