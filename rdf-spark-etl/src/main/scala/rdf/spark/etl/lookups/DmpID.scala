package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.DmpModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class DmpID()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new DmpModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.requestLookup()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.DMP_ID as DMP_ID_LOOKUP FROM od left outer join lookup on lookup.DMP_CD=od.DMP_ID)")
			.drop("DMP_ID")
			.withColumnRenamed("DMP_ID_LOOKUP", "DMP_ID")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where DMP_ID is null"))
	}

}
