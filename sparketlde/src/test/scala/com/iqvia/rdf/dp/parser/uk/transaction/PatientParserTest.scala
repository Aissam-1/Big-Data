package com.iqvia.rdf.dp.parser.uk.transaction

import com.iqvia.rdf.dp.parser.uk.BaseSpec
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PatientParserTest extends BaseSpec {

  private var patientParser: PatientParser = _
  private val spark = ss

  import spark.implicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    patientParser = new PatientParser()
  }

  /*
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


  "Mapper" should "be able to map a correct DataFrame" in {

    val list = Seq(("1", "19032897", "N", "000K", "20180403", "095000", "02uO", "null", "null", "000A", "SEX001", "20170300", "EDINBURGH", "MAR000", "RNK000", "REG002", "ACC002", "N", "20170424", "20170424", "0006", "0006", "FHS000", "null", "null", "PEX000", "null", "TRA000", "CAP000", "SRE000", "N", "null", "null", "6802"),
      ("2", "19032897", "N", "000K", "20180403", "095000", "02uO", "null", "null", "000A", "SEX001", "20170300", "EDINBURGH", "MAR000", "RNK000", "REG002", "ACC002", "N", "20170424", "20170424", "0006", "0006", "FHS000", "null", "null", "PEX000", "null", "TRA000", "CAP000", "SRE000", "N", "null", "null", "6802"),
      ("3", "19032897", "N", "000K", "20180403", "095000", "02uO", "null", "null", "000A", "SEX001", "20170300", "EDINBURGH", "MAR000", "RNK000", "REG002", "ACC002", "N", "20170424", "20170424", "0006", "0006", "FHS000", "null", "null", "PEX000", "null", "TRA000", "CAP000", "SRE000", "N", "null", "null", "6802")
    )

    val df = sc.parallelize(list).toDF()
    df.show(100)
  }*/



  "Mapper" should "be able to map a wrong DataFrame" in {

    val list = List(("100", 1.0), ("200", 2.0), ("300", 3.0))
    val row = Row.fromSeq(list)
    val rdd = sc.makeRDD(List(row))

    val fields = List(
      StructField("First Column", StringType, nullable = false),
      StructField("Second Column", DoubleType, nullable = false)
    )
    val df = ss.createDataFrame(rdd, StructType(fields))

    df.show()

    //df.show(100)
  }


  /*
    "I Should be Able to Read a CSV File" should "and it should have the right output" in {
      val toCobble = sc.parallelize(Seq("Shoe", "Boot", "Foo"))
      toCobble.map(dataCobbler.cobble).count should be(3)
      toCobble.map(dataCobbler.cobble).first should be("Cobble the Shoe")
    }
  */

}
