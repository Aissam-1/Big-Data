package rdf.spark.etl.dimensions

import com.imshealth.rdf.framework.spark.jobs.runner.Job
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import rdf.spark.etl.framework.operator.Dimension
import rdf.spark.etl.lookups._
import rdf.spark.etl.lookups._
import rdf.spark.etl.lookups.operate.Supplier
import rdf.spark.etl.models.oracle.dimensions.PatientModel
//import lookups.common.InputMap
import org.apache.spark.sql.SparkSession

class Patient extends Dimension with Job with LazyLogging{
	
	/**
	  * Runs the dimension process and declares the sequence of lookups to be used
	  *
	  * @param args optional array of string arguments
	  * @param conf implicit Config object which gathers the configuration file options for the whole process
	  * @param spark implicit SparkSession
	  */
	override def run(args: Array[String])(implicit conf: Config, spark: SparkSession): Unit = {
		this.setLookups(List(
			new Supplier("RWE_ODH.ODH_PATIENT", "PRAC_ID"),
			new PraID(),
			new GenID(),
			new InsType(),
			new ScaleCharges(),
			new PatStaID(),
			new PatMarID()
		))
		this.launch(new PatientModel())
	}
	
	override def preProcessing(dataframe: DataFrame)(implicit conf: Config,  spark: SparkSession): Option[DataFrame] = {
		dataframe.select("TRA_ID", "SUPP_ID", "PRAC_ID", "PAT_ID", "GEN_ID", "PAT_STA_ID", "PAT_MAR_ID", "INSURANCE_TYPE", "SCALE_CHARGES").show(20)
		None
	}
	
	override def postProcessing(dataframe: DataFrame)(implicit conf: Config,  spark: SparkSession): Option[DataFrame] = {
		dataframe.select("TRA_ID", "SUPP_ID", "PRAC_ID", "PAT_ID", "GEN_ID", "PAT_STA_ID", "PAT_MAR_ID", "INSURANCE_TYPE", "SCALE_CHARGES").show(20)
		None
	}
	
}