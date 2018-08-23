package rdf.spark.etl.framework.model.oracle

import java.io.File
import java.sql.SQLSyntaxErrorException

import rdf.spark.etl.framework.lib.{CommandLines, ResourcesLauncher, Session}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.{Config, ConfigFactory}

class OracleTest extends FlatSpec with Matchers  {

  val args: Array[String] = Array("--mode", "dev", "--dimensions", "Contact", "--supplier", "PDGI", "--country", "DE", "--provider", "INFOLINK")
  val cmd = new CommandLines(args, new ResourcesLauncher("defaults"))
  
  implicit val spark: SparkSession = SparkSession.builder.config(new SparkConf(false).setMaster("local[*]").setAppName("Test")).getOrCreate
  implicit val config: Config = ConfigFactory.parseFile(new File("src/main/resources/dev.properties"))

  "Load a table in Oracle" should "send back a Spark dataframe" in {
      val oracle = new Oracle("OD")
      oracle.load("RWE_ODH.ODH_PATIENT") shouldBe a [DataFrame]
  }

  it should "send back an exception when table does not exist in DB" in {
    val oracle = new Oracle("OD")
    an [SQLSyntaxErrorException] should be thrownBy oracle.load("RWE_ODH.ODH_")
  }

  "Request a table in Oracle" should "send back a Spark dataframe" in {
    val oracle = new Oracle("OD")
    oracle.request("(SELECT * FROM RWE_ODH.ODH_PATIENT) tmp") shouldBe a [DataFrame]
  }

  it should "send back an exception when table does not exist in DB" in {
    val oracle = new Oracle("OD")
    an [SQLSyntaxErrorException] should be thrownBy oracle.request("(SELECT * RWE_ODH.ODH_) tmp")
  }

}
