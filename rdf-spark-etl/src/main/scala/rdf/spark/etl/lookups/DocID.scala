package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.DoctorModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class DocID()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new DoctorModel
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.requestLookup()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, cast(lookup.DOC_ID as bigint) as DOC_ID_LOOKUP FROM od left outer join lookup on (lookup.DOC_EID=od.DOC_ID)")
			.drop("DOC_ID")
			.withColumnRenamed("DOC_ID_LOOKUP", "DOC_ID")
	}
	
	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where DOC_ID is null"))
	}
	
}
