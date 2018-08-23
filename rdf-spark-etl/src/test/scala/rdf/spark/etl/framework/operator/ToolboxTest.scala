package rdf.spark.etl.framework.operator

import rdf.spark.etl.dimensions.Contact
import rdf.spark.etl.framework.lib.{CommandLines, ResourcesLauncher, Session}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



class ToolboxTest extends FlatSpec with Matchers{
  
    val args: Array[String] = Array("--mode", "dev", "--dimensions", "Contact", "--supplier", "PDGI", "--country", "DE", "--provider", "INFOLINK")
    val cmd = new CommandLines(args, new ResourcesLauncher("defaults"))
    
    implicit val spark: SparkSession = SparkSession.builder.config(new SparkConf(false).setMaster("local[*]").setAppName("Test")).getOrCreate
    
    "norm function" should "format text" in {
        ToolBox.norm("test") should be ("TEST")
        ToolBox.norm("test  123") should be ("TEST 123")
        ToolBox.norm("test ") should be ("TEST")
    }
    
    "shorten function" should "substring text" in {
        ToolBox.shorten("azertyuiop", 4, "last") should be ("uiop")
        ToolBox.shorten("azertyuiop", 4, "first") should be ("azer")
        ToolBox.shorten("azertyuiop", 4, "idem") should be ("azer")
        ToolBox.shorten("azertyuiop", 4, "") should be ("azer")
        ToolBox.shorten("azertyuiop", 0, "first") should be ("")
        ToolBox.shorten("azertyuiop", -2, "first") should be ("")
        ToolBox.shorten("azertyuiop", 20, "first") should be ("azertyuiop")
    }
    
    "computeTable" should "launch Table instance" in {
        ToolBox.computeTable("Contact") shouldBe a [Contact]
    }
  
}
