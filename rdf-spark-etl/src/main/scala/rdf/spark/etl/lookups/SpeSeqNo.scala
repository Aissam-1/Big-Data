package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.SuppSpclModel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class SpeSeqNo()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new SuppSpclModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getSpeSeqNo()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.SPCL_SEQ_NO_LOOKUP as SPE_ID_LOOKUP FROM od left outer join lookup on lookup.SRC_SPCL_TEXT_ORIGL=od.REFERRAL_TO_SPE")
			.drop("SPCL_SEQ_NO")
			.withColumnRenamed("SPCL_SEQ_NO_LOOKUP", "SPCL_SEQ_NO")
	}
	
	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where SPCL_SEQ_NO is null"))
	}
	
}
