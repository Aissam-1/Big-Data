package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.SuppSpclModel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class SpeID()(implicit conf: Config, spark: SparkSession) extends Lookup{


	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new SuppSpclModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getSpeID()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.SPE_ID as SPE_ID_LOOKUP FROM od left outer join lookup on lookup.SRC_SPCL_TEXT=NORM(od.REFERRAL_TO_SPE)")
			.drop("SPE_ID")
			.withColumnRenamed("SPE_ID_LOOKUP", "SPE_ID")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where SPE_ID is null"))
	}

}
