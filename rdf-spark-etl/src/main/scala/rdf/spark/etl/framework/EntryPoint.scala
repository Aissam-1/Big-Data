package rdf.spark.etl.framework

import com.imshealth.rdf.framework.spark.jobs.runner.Job
import com.typesafe.scalalogging.slf4j.LazyLogging
import rdf.spark.etl.framework.lib.{CommandLines, ResourcesLauncher, Session}
import rdf.spark.etl.framework.operator.ToolBox
import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config

/**
  * Represents the entry point of the application after RDF Framework is launched
  *
  * @author Guillaume Payen
  * @version 1.0
  */
object EntryPoint extends Job with LazyLogging{
	
	/**
	  * Process the application
	  *
	  * @param args optional array of string arguments
	  * @param conf implicit Config object which gathers the configuration file options for the whole process
	  * @param spark implicit SparkSession
	  *
	  */
	override def run(args: Array[String])(implicit conf: Config, spark: SparkSession): Unit = {
		val cmd = new CommandLines(args, new ResourcesLauncher("defaults"))
		Session.instance.init(cmd)
		Session.instance.setEnv
		Session.instance.getDimensions.foreach(x => {
			val table = ToolBox.computeTable(x)
			table.run(Array())
		})
		
		spark.stop()
		
	}
	
}

