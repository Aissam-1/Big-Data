package rdf.spark.etl.framework.operator

import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.ODModel
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Represents the implementation of a dimension execution.
  * For the spotted dimension, data will be loaded into a Spark dataframe, and the job will loop over each lookup, updating the dataframe step by step
  *
  * @author Guillaume Payen
  * @version 1.0
  */
trait Dimension {
    
    private var lookups: List[Lookup] = List()
    
    /**
      * A list of lookups will need to be declared so that the process knows how the dimension shall be computed
      */
    def setLookups(lookupList: List[Lookup]): Unit = {
        this.lookups = lookupList
    }

    private var dataframe: DataFrame = _
    
    /**
      * Main Dimension process. From a dimension loaded in ODModel, the sequence of lookups are processed sequentially
      *
      * @param tableModel The model that serves as a reference to compute the dimension
      * @param conf implicit Config object which gathers the configuration file options for the whole process
      * @param spark implicit SparkSession
      */
    def launch(tableModel: ODModel)(implicit conf: Config,  spark: SparkSession): Unit = {
        dataframe = tableModel.load
        dataframe = preProcessing(dataframe).getOrElse(dataframe)
        dataframe.persist
        
        this.lookups.foreach(look => {
            look.setDataframe(dataframe)
            look.run(Array())
            dataframe = look.getDataframe().persist()
        })
    
        dataframe = postProcessing(dataframe).getOrElse(dataframe)
        dataframe.persist

    }
    
    /**
      * Pre-process the dataset before passing the lookups. This can be useful when filtering irrelevant lines for instance, in order to process the lookups with less data
      *
      * @param dataframe The Spark dataFrame to work with
      * @param conf implicit Config object which gathers the configuration file options for the whole process
      * @param spark implicit SparkSession
      * @return a dataframe if it was processed, None, otherwise
      */
    def preProcessing(dataframe: DataFrame)(implicit conf: Config,  spark: SparkSession): Option[DataFrame] = {
        None
    }
    
    /**
      * Post-process the dataset before passing the lookups. This can be useful when filtering irrelevant lines over some specific business rules
      *
      * @param dataframe The Spark dataFrame to work with
      * @param conf implicit Config object which gathers the configuration file options for the whole process
      * @param spark implicit SparkSession
      * @return a dataframe if it was processed, None, otherwise
      */
    def postProcessing(dataframe: DataFrame)(implicit conf: Config,  spark: SparkSession): Option[DataFrame] = {
        None
    }
    
    /**
      * Saving processing. Business rules may be written here in order to specify whether data is inserted, updated, or ignored
      *
      * @param dataframe The model that serves as a reference to compute the dimension
      * @param conf implicit Config object which gathers the configuration file options for the whole process
      * @param spark implicit SparkSession
      */
    def save(dataframe: DataFrame)(implicit conf: Config,  spark: SparkSession): Unit = {
    
    }



}
