package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.PracticeModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class PraID()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new PracticeModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getPra()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, cast(lookup.PRA_ID as bigint) as PRAC_ID_LOOKUP FROM od  left outer join lookup on lookup.PRA_EID=od.PRAC_ID")
			.drop("PRAC_ID")
			.withColumnRenamed("PRAC_ID_LOOKUP", "PRAC_ID")
	}
	
	override def getRejectedData(): Option[DataFrame] = {
		Some( spark.sql("select * from merged where PRAC_ID is null"))
	}
	
}
