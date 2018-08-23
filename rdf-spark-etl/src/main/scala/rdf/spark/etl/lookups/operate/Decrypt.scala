package rdf.spark.etl.lookups.operate

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import rdf.spark.etl.framework.lib.Session
import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.DoctorPracticeModel

class Decrypt()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new DoctorPracticeModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.requestLookup()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.PRA_EID AS PRAC_ID_LOOKUP, lookup.DOC_EID as DOC_ID_LOOKUP FROM od left outer join lookup on (lookup.DOC_EID=concat('" + Session.instance.getSupplierName + "-',od.DOC_ID) AND lookup.ENCRYTED_PRA_EID=od.PRAC_ID)")
			.drop("PRAC_ID", "DOC_ID")
			.withColumnRenamed("DOC_ID_LOOKUP", "DOC_ID")
			.withColumnRenamed("PRAC_ID_LOOKUP", "PRAC_ID")
	}
	
	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where PRAC_ID is null"))
	}
	
}
