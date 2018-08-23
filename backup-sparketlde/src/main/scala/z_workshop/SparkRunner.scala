package sparkworkshop

import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe

object SparkRunner {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    run(args, spark)
  }

  def run(args: Array[String], spark: SparkSession) : Unit = {
    println("SparkRunner started.")

    val sc = spark.sparkContext

    try {
      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule(args(0))
      val jobRunner = runtimeMirror
        .reflectModule(module)
        .instance
        .asInstanceOf[JobRunner]

      jobRunner run(args.tail, spark)
    } finally {
      spark stop()
    }

    println("SparkRunner finished.")
  }
}
