package sparkworkshop.exercises

import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkworkshop.JobRunner

/**
  * EXERCISE.
  * Read people data from people_extended.csv and sports and people_sports using readData function then filter people
  * with age < 104 using SQL API.
  * Next join people with sports using three datasets
  * (people, sports, people_sports) and select four columns (id, name, age, sport_name).
  */
object SparkSQLJoinsExercise extends JobRunner {

  case class Person(id : Long, name : String, age : Int)

  def readData(spark: SparkSession, fileName: String, fileFormat: String): DataFrame = {
    spark
      .read
      .format(fileFormat)
      .option("header", true)
      .option("inferSchema", "true")
      .load(fileName)
  }

  override def run(args: Array[String], spark: SparkSession): Unit = {
    // TODO
//    val people = readData(spark, "test-data/people_extended.csv", "csv")
//    val sports = readData(spark, "test-data/sports.csv", "csv")
//    val people_sports = readData(spark, "test-data/people_sports.csv", "csv")
//    import spark.implicits._
//
//    // joining many tables
//    val peopleWithSports =
//      people
//        .filter($"age" < 104)
//        .join(people_sports, people("id") === people_sports("person_id"))
//        .join(sports, people_sports("sport_id") === sports("id"))
//        .select(people("id"), people("name"), people("age"), sports("name").alias("sport_name"))
//
//    // printing useful runtime information
//    peopleWithSports.explain(true)
//
//    //peopleWithSports.printSchema
//
//    peopleWithSports.show
  }
}
