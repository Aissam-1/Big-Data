package rdf.spark.etl.lookups

import com.typesafe.config.Config
import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.SuppTestModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class TstSeqNo()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new SuppTestModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getTstSeqNo()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, lookup.TST_SEQ_NO as TST_SEQ_NO_LOOKUP FROM od left outer join lookup on (lookup.SRC_TST_TEXT=NORM(od.TST_NOTE) AND lookup.SRC_VALUE_TYPE=od.TST_VALUE_TYPE AND lookup.SRC_TEST_ABBR=od.TST_TEXT)")
			.drop("TST_SEQ_NO")
			.withColumnRenamed("TST_SEQ_NO_LOOKUP", "TST_SEQ_NO")
	}

	override def getRejectedData(): Option[DataFrame] = {
		Some(spark.sql("select * from merged where TST_SEQ_NO is null"))
	}

}
