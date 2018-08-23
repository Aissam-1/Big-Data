package rdf.spark.etl.framework.operator

import com.imshealth.rdf.framework.spark.jobs.runner.Job
import org.apache.spark.sql.SparkSession

/**
  * Represents a set of tool functions that may be used all along the process.
  *
  * @author Guillaume Payen
  * @version 1.0
  */
object ToolBox {
	
	/**
	  * Launch a dimension class from the name of its class
	  *
	  * @param tableName The name of the dimension
	  * @return The dimension class instance
	  */
	def computeTable(tableName: String)(implicit spark: SparkSession): Job = {
		val action = Class.forName("rdf.spark.etl.dimensions." + tableName).newInstance()
		action.asInstanceOf[Job]
	}
	
	
	/**
	  * Format some text. This method shall be used as a stored procedure for Spark SQL
	  *
	  * @param text The text that has to be formatted
	  * @return The updated text
	  */
	def norm(text: String): String = {
		val asciiControlChars: String = "([\\x00-\\x1F])"
		val asciiPrintableChars: String = "([\\x22\\x27\\x5E\\x60\\x7F])"
		val asciiExtendedChars: String = "([\\x82\\x84\\x86\\x87\\x88\\x8B\\x8D\\x8F\\x90\\x91\\x92\\" + "x93\\x94\\x95\\x96\\x97\\x98\\x9B\\x9D\\xA0\\xA8\\xAB\\xAC\\xAD\\xAF\\xB4\\xB6\\xB7\\xB8\\xBA\\xBB])"
		text.replaceAll(asciiControlChars + "|" + asciiPrintableChars + "|" + asciiExtendedChars, "")
			.replaceAll(" {2,}", " ")
			.toUpperCase
			.trim
	}
	
	/**
	  * Get a substring of the provided text. This method shall be used as a stored procedure for Spark SQL
	  *
	  * @param text The text that has to be formatted
	  * @param n The number of characters for the substring
	  * @param start Shall the substring perform from the first n characters, or the last ?
	  * @return The substring text
	  */
	def shorten(text: String, n: Int, start: String): String = {
		var m = n
		if (n<0) m = 0
		start match {
			case "last" => text takeRight m
			case _ => text take m
		}
	}
	
	/**
	  * Get the sysdate. This method shall be used as a stored procedure for Spark SQL
	  * @return The system date
	  */
	def getSysDate(): String = {
		val format = new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
		format.format(new java.util.Date())
	}
	
	
}
