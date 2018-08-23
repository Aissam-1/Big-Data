package rdf.spark.etl.framework.model.oracle

import java.util.Properties

import com.imshealth.rdf.framework.spark.jobs.read.{DbReadContext, DbReader}
import com.imshealth.rdf.framework.spark.utils.Connection
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}



/**
  * Represents the main interaction between Oracle and Spark. This class is the input and the output for Spark
  *
  * @constructor Creates a new Oracle model that will be plugged into Spark environment.
  *              mde argument allows to specify whether Oracle database shall be accessed in dev, preprod or prod mode
  
  * @param conf implicit Config object which gathers the configuration file options for the whole process
  * @param spark implicit SparkSession
  *
  * @author Guillaume Payen
  * @version 1.0
  */
class Oracle(mode: String)(implicit conf: Config, spark: SparkSession) extends DbReader {
	
	/*private val oracleUrl: String =  if (this.mode == "OD")
		conf.getString("oracle.url.od")
	else
		conf.getString("oracle.url.th")*/
	
	private val oracleUrl: String =  if (this.mode == "OD")
		conf.getString("oracle.url.od")
	else
		conf.getString("oracle.url.th")
	private val oracleDriver: String = conf.getString("oracle.driver")
	private val oracleUsername: String = conf.getString("oracle.username")
	private val oraclePassword: String = conf.getString("oracle.password")
	
	val connection: Connection = new Connection(oracleUrl, conf.getString("oracle.driver"), conf.getString("oracle.username"), conf.getString("oracle.password"), conf.getLong("dimension.limit"))
	
	/**
	  * Load a specific table (SELECT *) from Oracle and loads it into a Spark dataframe
	  *
	  * @param table The Oracle table name
	  * @return Spark Dataframe
	  */
	def load(table: String): DataFrame = {
		this.read(new DbReadContext(
			this.connection,
			"(SELECT * FROM " + table + " ) foo", conf.getLong("dimension.limit")
		))
	}
	
	/**
	  * Load data from a specific request from Oracle and loads it into a Spark dataframe
	  *
	  * @param request The Oracle request to operate on database
	  * @return Spark Dataframe
	  */
	def request(request: String): DataFrame = {
		/*this.read(new DbReadContext(
			this.connection,
			request, conf.getLong("dimension.limit")
		))*/
		spark.read
			.format("jdbc")
			.option("url", oracleUrl)
			.option("driver", oracleDriver)
			.option("dbtable", request)
			.option("user", oracleUsername)
			.option("password", oraclePassword)
			.load()
		
	}
	
	/**
	  * Save a dataframe into Oracle database
	  *
	  * @param dataframe The Spark dataframe to save
	  * @param table The table to save the data into
	  */
	def save(dataframe: DataFrame, table: String, connectionProperties: Properties): Unit = {
		/*connectionProperties.put("user", oracleUsername)
		connectionProperties.put("password", oraclePassword)
		connectionProperties.put("driver", oracleDriver)
		dataframe
			.write
			.mode(SaveMode.Append)
			.jdbc(oracleUrl, table, connectionProperties)*/
	}
	
}
