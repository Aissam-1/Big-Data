package z_myapp

import java.sql.{Connection, DriverManager}

object TestH2 {

  private def createDbStructure(conn: Connection): Unit = {

    val sql =
      """
      create schema if not exists RWE_OD;

      set schema RWE_OD;

      create table if not exists OD_PATIENT_TMP (
       TRA_ID NUMBER,
       SUPP_ID NUMBER,
       PRAC_ID VARCHAR2(4000 CHAR),
       PAT_ID VARCHAR2(4000 CHAR),
       BIRTH_YEAR VARCHAR2(4000 CHAR),
       BIRTH_MONTH VARCHAR2(4000 CHAR),
       GEN_ID VARCHAR2(4000 CHAR),
       PAT_STA_ID VARCHAR2(4000 CHAR),
       PAT_MAR_ID VARCHAR2(4000 CHAR),
       SUB_ID VARCHAR2(4000 CHAR),
       REGISTRATION_DATE VARCHAR2(4000 CHAR),
       REGISTRATION_OUT_DATE VARCHAR2(4000 CHAR),
       MORTALITY_DATE VARCHAR2(4000 CHAR),
       WEIGHT VARCHAR2(4000 CHAR),
       HEIGHT VARCHAR2(4000 CHAR),
       INTEGRATED_CARE VARCHAR2(4000 CHAR),
       INTEGRATED_CARE_TYPE VARCHAR2(4000 CHAR),
       INSURANCE_TYPE VARCHAR2(4000 CHAR),
       SCALE_CHARGES VARCHAR2(4000 CHAR),
       HEALTH_INSURANCE VARCHAR2(4000 CHAR),
       TRA_ID_ORI NUMBER,
       REJECT_COUNT NUMBER,
       BMI VARCHAR2(4000 CHAR),
       BSA VARCHAR2(4000 CHAR),
       ETHNICITY_ID VARCHAR2(4000 CHAR),
       EXITUS VARCHAR2(4000 CHAR),
       INFORM_CONSENT_SIGNED VARCHAR2(4000 CHAR),
       INVESTIGATOR VARCHAR2(4000 CHAR),
       AUDITFLAG VARCHAR2(4000 CHAR),
       AUDITSEQ VARCHAR2(4000 CHAR),
       AUDITED VARCHAR2(4000 CHAR),
       OPERAT_ID VARCHAR2(4000 CHAR),
       SRC_SYSDATE VARCHAR2(4000 CHAR),
       SRC_SYSTIME VARCHAR2(4000 CHAR),
       RI_CODE VARCHAR2(4000 CHAR),
       DHA VARCHAR2(4000 CHAR),
       FHSA VARCHAR2(4000 CHAR),
       BIRTHPLACE VARCHAR2(4000 CHAR),
       HOUSE_RANK VARCHAR2(4000 CHAR),
       REG_STATUS VARCHAR2(4000 CHAR),
       ACCEPTANCE VARCHAR2(4000 CHAR),
       PROVISIONA VARCHAR2(4000 CHAR),
       APPLIED VARCHAR2(4000 CHAR),
       REG_GP VARCHAR2(4000 CHAR),
       USUAL_GP VARCHAR2(4000 CHAR),
       PREV_FHSA VARCHAR2(4000 CHAR),
       VAMP_OLDID VARCHAR2(4000 CHAR),
       DISPENSING VARCHAR2(4000 CHAR),
       PRESCRIBE VARCHAR2(4000 CHAR),
       CAPP_SUPP VARCHAR2(4000 CHAR),
       SOURCE_REG VARCHAR2(4000 CHAR),
       CHSREG VARCHAR2(4000 CHAR),
       CHSDR VARCHAR2(4000 CHAR),
       CHSDATE VARCHAR2(4000 CHAR),
       FAMILY_NO VARCHAR2(4000 CHAR));"""
    val stmt = conn.createStatement()
    try {
      stmt.execute(sql)
    }
    finally {
      stmt.close
    }

  }

  def main(args: Array[String]): Unit = {
    Class.forName("org.h2.Driver")
    val conn: Connection = DriverManager.getConnection("jdbc:h2:./db/RWE_OD", "sa", "")
    try {
      createDbStructure(conn)
      println("ok")
    }
    finally {
      conn.close()
    }
  }
}
