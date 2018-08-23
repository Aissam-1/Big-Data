package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PracticeModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{

  def getPat(): DataFrame = {
      super.request("(SELECT DISTINCT PRA_EID, PAT_ID, PAT_EID FROM RWE_DE.TH_PATIENT_PRACTICE) TH_PRACTICE")
  }

  def getPra(): DataFrame = {
      super.request("(SELECT DISTINCT PRA_EID, PRA_ID FROM RWE_DE.TH_PRACTICE) TH_PRACTICE")
  }

}
