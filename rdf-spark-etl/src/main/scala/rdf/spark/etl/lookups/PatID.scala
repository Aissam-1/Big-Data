package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.PracticeModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class PatID()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new PracticeModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getPat()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, cast(lookup.PAT_ID as bigint) as PAT_ID_LOOKUP FROM od left outer join lookup on (lookup.PRA_EID=od.PRAC_ID AND lookup.PAT_EID=od.PAT_ID)")
			.drop("PAT_ID")
			.withColumnRenamed("PAT_ID_LOOKUP", "PAT_ID")
	}
	
	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where PAT_ID is null"))
	}
	
}
