package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import rdf.spark.etl.models.oracle.lookups.TestModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class TstUnitID()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new TestModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getTstID()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.TST_UNIT_ID as TST_UNIT_ID_LOOKUP FROM od left outer join lookup on (lookup.TEST_UNIT_CD=od.TST_UNIT")
			.drop("TST_UNIT_ID")
			.withColumnRenamed("TST_UNIT_ID_LOOKUP", "TST_UNIT_ID")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where TST_UNIT_ID is null"))
	}

}
