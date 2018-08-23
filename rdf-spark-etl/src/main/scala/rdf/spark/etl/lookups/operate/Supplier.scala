package rdf.spark.etl.lookups.operate

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import rdf.spark.etl.framework.lib.Session
import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.DoctorPracticeModel

class Supplier(table: String, field: String)(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new DoctorPracticeModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.requestLookup()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, concat('" + Session.instance.getSupplierName + "-',od."+field+") AS SUPPLIER_LOOKUP FROM od")
			.drop(field)
			.withColumnRenamed("SUPPLIER_LOOKUP", field)
	}
	
}
