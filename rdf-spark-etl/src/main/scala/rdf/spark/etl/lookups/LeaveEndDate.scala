package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class LeaveEndDate()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, SUFFIX(SICK_LEAVE_DURATION, 8) as SICK_LEAVE_DURATION_LOOKUP")
			.drop("SICK_LEAVE_DURATION")
			.withColumnRenamed("SICK_LEAVE_DURATION_LOOKUP", "SICK_LEAVE_DURATION")
	}

}