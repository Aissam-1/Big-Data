package rdf.spark.etl.framework.lib

import com.typesafe.config.{ConfigException, ConfigFactory}

/**
  * Represents the launcher of configuration files located in resources folder
  *
  * @constructor Creates a new resources manager with a configuration file name
  *              The configuration name does not take extension of the file.
  *              To load "resources/dev.properties", just use "dev"
  * @author Guillaume Payen
  * @version 1.0
  */
class ResourcesLauncher(resourceName: String) {

  /**
    * The config loader
    */
    private val parameters = ConfigFactory.load(resourceName)
    
    /**
    * Get the value for the key specified as an argument, in the configuration file
    *
    * @param key The key to fetch
    * @return The matching value for the key if it exists,
    *         "None" otherwise
    */
    def get(key: String): Option[String] = {
        try {
            Some(parameters.getString(key))
        }
        catch{
            case configException: ConfigException => None
        }
    }
    
    /**
    * Get the value for the key specified as an argument, in the configuration file
    * The key is supposed to be a sequence of items with a comma separator
    * ie. "dim1, dim2, dim3, dim4"
    *
    * @param key The key to fetch
    * @return The matching value for the key if it exists, converted into a sequence,
    *         "None" otherwise
    */
    def getSeq(key: String): Option[Seq[String]] = {
        try {
            Some(parameters.getString(key).split(",").toSeq.filter(_.nonEmpty))
        }
        catch{
            case configException: ConfigException => None
        }
    }

}

