package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.SuppTestModel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class TstID()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new SuppTestModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getTstID()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.TST_ID as TST_ID_LOOKUP FROM od left outer join lookup on (lookup.SRC_TST_TEXT=NORM(od.TST_NOTE) AND lookup.SRC_VALUE_TYPE=od.TST_VALUE_TYPE AND lookup.SRC_TEST_ABBR=od.TST_TEXT)")
			.drop("TST_ID")
			.withColumnRenamed("TST_ID_LOOKUP", "TST_ID")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where TST_ID is null"))
	}

}
