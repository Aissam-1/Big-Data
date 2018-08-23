package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.SuppSpclModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class PreSpeID()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new SuppSpclModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getPreSpeId()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.PRE_SPE_ID as PRE_SPE_ID_LOOKUP FROM od left outer join lookup on lookup.SRC_SPCL_TEXT=NORM(od.PRE_SPE_ID)")
			.drop("PRE_SPE_ID")
			.withColumnRenamed("PRE_SPE_ID_LOOKUP", "PRE_SPE_ID")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where PRE_SPE_ID is null"))
	}

}
