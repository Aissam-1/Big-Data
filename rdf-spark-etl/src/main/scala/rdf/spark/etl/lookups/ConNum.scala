package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class ConNum()(implicit conf: Config, spark: SparkSession) extends Lookup{

    override def lookupProcess(dataframe: DataFrame): DataFrame = {
        dataframe.createOrReplaceTempView("od")
        spark.sql("SELECT od.*, IIF(isnull(CON_NUM),1,CON_NUM) as CON_NUM_LOOKUP")
            .drop("CON_NUM")
            .withColumnRenamed("CON_NUM_LOOKUP", "CON_NUM")
    }

}