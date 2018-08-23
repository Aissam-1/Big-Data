package com.iqvia.rdf.dp.parser.uk

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

abstract class BaseSpec extends FlatSpec with BeforeAndAfterAll with Matchers {
  private val master = "local"
  private val appName = "testing"
  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  var ss: SparkSession = _

  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.allowMultipleContexts", "true")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sc = SparkContext.getOrCreate(conf)
    sqlContext = SQLContext.getOrCreate(sc)
    ss = SparkSession.builder().config(conf).getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
      sc = null
      sqlContext = null
      ss.stop()
      ss = null
    } finally {
      super.afterAll()
    }
  }
}