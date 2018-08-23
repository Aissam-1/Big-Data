package rdf.spark.etl.lookups

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import rdf.spark.etl.framework.operator.Lookup
import rdf.spark.etl.models.oracle.lookups.ContactModel

class ConID()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		val lookupModel = new ContactModel()
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = lookupModel.getContact()
		lookup.createOrReplaceTempView("lookup")
		spark.sql("SELECT od.*, cast(lookup.CON_ID as bigint) as CON_ID_LOOKUP FROM od left outer join lookup on (lookup.DOC_ID=od.DOC_ID AND lookup.PAT_ID=od.PAT_ID AND od.CON_DATE=lookup.CON_DATE)")
	}
	
}