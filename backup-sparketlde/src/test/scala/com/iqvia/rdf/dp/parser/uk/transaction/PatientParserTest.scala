package com.iqvia.rdf.dp.parser.uk.transaction

import com.iqvia.rdf.dp.parser.uk.BaseSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PatientParserTest extends BaseSpec {

  private var patientParser: PatientParser = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    patientParser = new PatientParser()
  }

  "Reader" should "successfully read a CSV File and have the right number of lines" in {
    patientParser.read(ss, "src/test/resources/csv/PATIENT.csv").count() should be(29)
  }

  "Reader" should "successfully read a CSV file and have the right content" in {
    val firstRow: String = "[1,19032897,N,000K,20180403,095000,02uO,null,null,000A,SEX001,20170300,EDINBURGH,MAR000,RNK000,REG002,ACC002,N,20170424,20170424,0006,0006,FHS000,null,null,PEX000,null,TRA000,CAP000,SRE000,N,null,null,6802]"
    patientParser.read(ss, "src/test/resources/csv/PATIENT.csv").first.toString() should be(firstRow)
  }

  "Reader" should "be able to manage a not found CSV file" in {
    patientParser.read(ss, "src/test/resources/csv/NOFILE") should be(null)
  }

  "Reader" should "be able to manage a corrupted file" in {
    patientParser.read(ss, "src/test/resources/csv/IMAGE.PNG").toString() should be("[_c0: string]")
  }



/*
  "I Should be Able to Read a CSV File" should "and it should have the right output" in {
    val toCobble = sc.parallelize(Seq("Shoe", "Boot", "Foo"))
    toCobble.map(dataCobbler.cobble).count should be(3)
    toCobble.map(dataCobbler.cobble).first should be("Cobble the Shoe")
  }
*/

}
