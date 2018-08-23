package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.DiaModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class DiaID()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new DiaModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.requestLookup()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.DIA_ID as DIA_ID_LOOKUP FROM od left outer join lookup on (lookup.SRC_DIAG_CD=od.SRC_DIAG_CS_IN AND lookup.SRC_DIAG_CERTAINTY_CD=od.DIA_CERTAINTY AND lookup.SRC_DIAG_LOC_CD=od.DIA_SIDE_LOCALIZATION AND lookup.SRC_DIAG_DEG_CD=od.DIA_DEGREE AND lookup.SRC_DIAG_DESC_ORIGL=od.DIAG_DESC_ORIGL AND lookup.SRC_DIAG_EXPLN_TXT_ORIGL=od.DIAG_EXPLN_TEXT_ORIGL)")
			.drop("DIA_ID")
			.withColumnRenamed("DIA_ID_LOOKUP", "DIA_ID")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where DIA_ID is null"))
	}

}
