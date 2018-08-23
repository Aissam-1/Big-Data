package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.SuppProdModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrdID()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new SuppProdModel
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getPrID()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.PRD_ID as PRD_ID_LOOKUP FROM od left outer join lookup on (lookup.SRC_PROD_CD=NORM(od.DRUG_ADDITIONAL_INFO_1) AND lookup.SRC_PACK_SIZE=NORM(od.DRUG_ADDITIONAL_INFO_3) AND lookup.SRC_PROD_LNG_TXT_ORIGL=od.DRUG_ADDITIONAL_INFO_2)")
			.drop("PRD_ID")
			.withColumnRenamed("PRD_ID_LOOKUP", "PRD_ID")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where PRD_ID is null"))
	}

}
