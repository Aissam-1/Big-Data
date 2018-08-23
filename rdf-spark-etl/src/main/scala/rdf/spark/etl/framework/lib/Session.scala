package rdf.spark.etl.framework.lib

import com.typesafe.config.Config
import rdf.spark.etl.framework.operator.ToolBox
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Represents the Session loader, where SparkSession will be instanciated once for all, and resources necessary to process the dimensions
  * This class follows the singleton pattern because the Spark Session shall be instanciated only once so that all the Spark processes are distributed within the same context.
  *
  * @author Guillaume Payen
  * @version 1.0
  */
class Session private() {
	
	/**
	  * Shortcut to fetch the supplier name
	  */
	private var supplierName: String = _
	
	/**
	  * Shortcut to fetch the country code
	  */
	private var countryCode: String = _
	
	/**
	  * Shortcut to fetch the provider name
	  */
	private var providerName: String = _
	
	/**
	  * Get the Spark Session
	  */
	private var spark: SparkSession = _
	
	private var dimensions: Seq[String] = List()
	
	/**
	  * Initialize the command line options in a Singleton object
	  *
	  * @param cmd CommandLine instance used to fetch the job information that has been inputted when launching the job
	  */
	def init(cmd: CommandLines): Unit = {
		this.supplierName = cmd.getSupplierName
		this.providerName = cmd.getProviderName
		this.countryCode = cmd.getCountryCode
		this.dimensions = cmd.getTables
	}
	
	/**
	  * Initialize the SparkSession environment, with level of logging, and stored procedures
	  *
	  * @param conf implicit Config object which gathers the configuration file options for the whole process
	  * @param spark implicit SparkSession
	  */
	def setEnv(implicit conf: Config, spark: SparkSession): Unit = {
		spark.udf.register("NORM", (text: String) => ToolBox.norm(text))
		spark.udf.register("PREFIX", (text: String, n: Int) => ToolBox.shorten(text, n, "first"))
		spark.udf.register("SUFFIX", (text: String, n: Int) => ToolBox.shorten(text, n, "last"))
		Logger
			.getLogger("org")
			.setLevel(Level.toLevel(conf.getString("job.log.level")))
	}
	
	/**
	  * Get the supplier name for the job
	  */
	def getSupplierName: String = this.supplierName
	
	/**
	  * Get the provider name for the job
	  */
	def getProviderName: String = this.providerName
	
	/**
	  * Get the country code for the job
	  */
	def getCountryCode: String = this.countryCode
	
	/**
	  * Get the dimensions for the job
	  */
	def getDimensions: Seq[String] = this.dimensions
	
}

object Session {
	private var _instance : Session = null
	def instance = {
		if (_instance == null)
			_instance = new Session()
		_instance
	}
}