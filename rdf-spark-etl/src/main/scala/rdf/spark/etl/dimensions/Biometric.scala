package rdf.spark.etl.dimensions

import com.imshealth.rdf.framework.spark.jobs.runner.Job
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import rdf.spark.etl.framework.operator.Dimension
import rdf.spark.etl.lookups._
import rdf.spark.etl.lookups.operate.Decrypt
//import lookups.common.InputMap
import org.apache.spark.sql.SparkSession
import rdf.spark.etl.models.oracle.dimensions.ContactModel

/**
  * Represents the Contact dimension
  *
  * Methods setDimension, setLookups are to be overriden as imposed by the trait Dimension
  * @author Guillaume Payen
  * @version 1.0
  */
class Biometric extends Dimension with Job with LazyLogging{
	
	/**
	  * Runs the dimension process and declares the sequence of lookups to be used
	  *
	  * @param args optional array of string arguments
	  * @param conf implicit Config object which gathers the configuration file options for the whole process
	  * @param spark implicit SparkSession
	  */
	override def run(args: Array[String])(implicit conf: Config, spark: SparkSession): Unit = {
	    this.setLookups(List(
		    new Decrypt(),
	        new PraID(),
	        new DocID(),
		    new SmokerStatus(),
	        new SmokerSymptom(),
	        new AdipositySymptom(),
		    new ConID()
		    //new InputMap(Map("PRAC_ID" -> "test", "PAT_ID" -> "50"))
	    ))
	    this.launch(new ContactModel())
    }
	
	override def preProcessing(dataframe: DataFrame)(implicit conf: Config,  spark: SparkSession): Option[DataFrame] = {
		dataframe.select("TRA_ID", "PRAC_ID", "PAT_ID", "DOC_ID", "CON_DATE", "SMOKER_STATUS", "SMOKER_SYMPTOM", "ADIPOSITY_SYMPTOM").show(20)
		None
	}
	
	override def postProcessing(dataframe: DataFrame)(implicit conf: Config,  spark: SparkSession): Option[DataFrame] = {
		dataframe.select("TRA_ID", "PRAC_ID", "PAT_ID", "DOC_ID", "CON_DATE", "SMOKER_STATUS", "SMOKER_SYMPTOM", "ADIPOSITY_SYMPTOM", "CON_ID_LOOKUP").show(20)
		None
	}
	
}