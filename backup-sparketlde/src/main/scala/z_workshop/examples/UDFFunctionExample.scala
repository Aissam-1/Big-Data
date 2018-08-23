package sparkworkshop.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFFunctionExample extends sparkworkshop.JobRunner {

  override def run(args: Array[String], spark: SparkSession): Unit = {

    import spark.implicits._

    // UDF
    val myUDF: String => String = _.toUpperCase
    val toUpperCaseUdf = udf(myUDF)

    Seq("abc1", "bcde2", "cd", "Deee", "err", "fini")
      .toDF("name")
      .withColumn("UPPER(name)", toUpperCaseUdf($"name"))
      .show()
  }

}
