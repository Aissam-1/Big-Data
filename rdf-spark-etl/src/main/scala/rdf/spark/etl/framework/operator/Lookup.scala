package rdf.spark.etl.framework.operator

import com.imshealth.rdf.framework.spark.jobs.runner.Job
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

/**
  * Represents the implementation of a lookup execution.
  *
  * @author Guillaume Payen
  * @version 1.0
  */
abstract class Lookup extends Job with LazyLogging {
    
    var dataframe: DataFrame = _
    
    /**
      * Saves a dataframe into the class attribute
      *
      * @param dataframe the DataFrame to be saved
      *
      */
    def setDataframe(dataframe: DataFrame): Unit = {
        this.dataframe = dataframe
    }
    
    /**
      * Returns the class dataframe variable
      *
      * @return DataFrame
      *
      */
    def getDataframe(): DataFrame = this.dataframe
    
    /**
      * The lookup class shall implement the Spark operations that will allow the update of the dimension dataframe
      *
      * @param dataframe The dimension Spark dataframe
      * @return an updated version of the dataframe
      */
    def lookupProcess(dataframe: DataFrame): DataFrame

    /**
      * The lookup class may implement the Spark operations that will build the rejection dataframe.
      * The implmentation of this method is not mandatory. If not overriden, there will be no rejected data for this lookup.
      *
      * @return a rejection dataframe, or None if the method is not overriden.
      */
    def getRejectedData(): Option[DataFrame] = {
        None
    }

    /**
      * The lookup processing operates 3 steps. The dimension dataframe will be updated in accordance with the lookup rules. Then, the rejection dataframe will be computed and sent to the rejection model.
      * Finally, the dimension dataframe will be purged from the rejection dataframe.
      *
      * @param args optional array of string arguments
      * @param conf implicit Config object which gathers the configuration file options for the whole process
      * @param spark implicit SparkSession
      */
    override def run(args: Array[String])(implicit conf: Config, spark: SparkSession): Unit = {
        val merged: DataFrame = this.lookupProcess(dataframe)
        merged.createOrReplaceTempView("merged")
        merged.persist()
        val rejected: Option[DataFrame] = this.getRejectedData()
        
        // TODO : move rejected to reject table
        //merged.select("GEN_ID", "INSURANCE_TYPE", "SCALE_CHARGES", "PAT_ID", "PAT_MAR_ID", "PAT_STA_ID", "PRAC_ID", "SUPP_ID", "TRA_ID", "TRA_ID_ORI").show(5)
        
        try {
            this.dataframe = merged.except(rejected.getOrElse(spark.emptyDataFrame))
        }
        catch{
            case analysisException: AnalysisException => this.dataframe = merged
        }
    }

}
