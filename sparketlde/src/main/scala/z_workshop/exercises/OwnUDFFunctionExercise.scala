package sparkworkshop.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
  * EXERCISE.
  * Define UDF function which will return size of string.
  * Then transpose a seq of strings into DF and show the result.
  * DF should contain two columns: initial with word and second one with word length.
  */
object OwnUDFFunctionExercise extends sparkworkshop.JobRunner {

  override def run(args: Array[String], spark: SparkSession): Unit = {

    // TODO:
//    import spark.implicits._
//
//    val myUDF: String => Int = _.size
//    val wordLength = udf(myUDF)
//
//    Seq("abc1", "bcde2", "cd", "Deee", "err", "fini")
//      .toDF("name")
//      .withColumn("length(name)", wordLength($"name"))
//      .show()

  }

}

