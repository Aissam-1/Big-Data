package rdf.spark.etl.framework.model.oracle

import org.apache.spark.sql.DataFrame

/**
  * Represents OD model, and forces the OD dimensions to override the following declared functions
  *
  * @author Guillaume Payen
  * @version 1.0
  */
trait ODModel {
	
	/**
	  * Classes that will implement this method may have to define the way data will be extracted from Oracle and converted into a Spark dataframme
	  *
	  * @return a Spark Dataframe
	  */
	def load: DataFrame
	
	/**
	  * Classes that will implement this method may have to define the way the Spark dataframe will be saved into Oracle
	  *
	  * @param data a Spark dataframe
	  */
	def save(data: DataFrame): Unit
	
	/**
	  * Classes that will implement this method may have to define the way the rejected Spark dataframe will be saved into Oracle
	  *
	  * @param data a Spark dataframe
	  */
	def reject(data: DataFrame): Unit
	
}
