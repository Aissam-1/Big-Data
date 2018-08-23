package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.SuppProdModel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProdSeqNo()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new SuppProdModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getProdSeqNo()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.PROD_SEQ_NO as PROD_SEQ_NO_LOOKUP FROM od left outer join lookup on (lookup.SRC_PROD_CD=NORM(od.DRUG_ADDITIONAL_INFO_1) AND lookup.SRC_PACK_SIZE=NORM(od.DRUG_ADDITIONAL_INFO_3) AND lookup.SRC_PROD_LNG_TXT_ORIGL=od.DRUG_ADDITIONAL_INFO_2)")
			.drop("PROD_SEQ_NO")
			.withColumnRenamed("PROD_SEQ_NO_LOOKUP", "PROD_SEQ_NO")
	}
	
	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where PROD_SEQ_NO is null"))
	}
	
}
