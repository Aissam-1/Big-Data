package com.iqvia.rdf.dp.parser.uk

import java.util.Properties

import com.iqvia.rdf.dp.parser.uk.transaction.{ContactParser, DoctorParser, PatientParser}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object AppLauncher {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("UK Parser")
      .config("spark.master", "local[*]")
      .config("spark.executor.memory", "4g")
      .getOrCreate()


    val conf = ConfigFactory.load("dev.application.properties")
    val connectionProperties = new Properties()
    connectionProperties.put("driver", conf.getString("st.driver"))
    connectionProperties.put("url", conf.getString("st.url"))
    connectionProperties.put("user", conf.getString("st.username"))
    connectionProperties.put("password", conf.getString("st.password"))

    /**
      * Patient
      */
    val patientParser = new PatientParser()
    val patIn = patientParser.read(spark, "test-data/PATIENT.*")
    val patOut = patientParser.map(patIn, spark)
    patientParser.write(patOut, "RWE_OD.OD_PATIENT_TMP", connectionProperties)

/*
    /**
      * Contact
      */
    val contactParser = new ContactParser()
    val conIn = contactParser.read(spark, "test-data/CONSULT.*")
    val conOut = patientParser.map(conIn, spark)
    patientParser.write(conOut, "RWE_OD.OD_CONTACT_TMP", connectionProperties)


    /**
      * Doctor
      */
    val doctorParser = new DoctorParser()
    val docIn = doctorParser.read(spark, "test-data/STAFF.*")
    val docOut = doctorParser.map(docIn, spark)
    patientParser.write(docOut, "RWE_OD.OD_DOCTOR_TMP", connectionProperties)
*/
  }
}
