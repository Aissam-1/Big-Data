package rdf.spark.etl.framework.lib

import org.scalatest.{FlatSpec, Matchers}

class CommandLinesTest  extends FlatSpec with Matchers {
  
    "Initialisation" should "be OK when args are set" in {
        new CommandLines(Array("--mode", "dev", "--dimensions", "Contact", "--supplier", "PDGI", "--country", "DE", "--provider", "INFOLINK"), new ResourcesLauncher("defaults")) shouldBe a [CommandLines]
    }
    
    it should "be OK when no args are set" in {
        new CommandLines(Array(), new ResourcesLauncher("defaults")) shouldBe a [CommandLines]
    }
    
    it should "throw exception when fake args are set" in {
        val c = new CommandLines(Array("--mode", "test"), new ResourcesLauncher("defaults"))
        an [NullPointerException] should be thrownBy c.getMode
    }
  
}
