package rdf.spark.etl.framework.lib

// https://github.com/scopt/scopt

/**
  * Represents the Command lines parameters management when launching the job
  *
  * @constructor Create a new command line management system
  *              args is an array of command line arguments set when launching the job
  *              resource is a config file executor that allows to fetch information for job execution in dev, preprod or prod mode
  * @author Guillaume Payen
  * @version 1.0
  */
class CommandLines(args: Array[String], resource: ResourcesLauncher) {

    /**
      * Definition of the command line arguments allowed for the job
      * If command line arguments are not set when executing the job, default values will be used
      * @see  resources/defaults.properties
      */
    private case class Config(
         mode: String = resource.get("commandline.mode").getOrElse("ide"),
         env: String = resource.get("commandline.mode").getOrElse("dev"),
         supplier: String = resource.get("commandline.supplier").getOrElse(""),
         provider: String = resource.get("commandline.provider").getOrElse(""),
         APPLICATION_CONF_LOCATION: String = resource.get("commandline.APPLICATION_CONF_LOCATION").getOrElse(""),
         country: String = resource.get("commandline.country").getOrElse(""),
         tables: Seq[String] = resource.getSeq("commandline.tables").getOrElse(Seq())
     )

    private var cfg: Config = _

    /**
      * Definition of the way arguments may be handled
      */
    private val parser = new scopt.OptionParser[Config]("scopt") {
        head("IQVIA ETL", "1.0")
        opt[String]('m', "mode")
            .valueName("<ide>, or <cluster>")
            .action( (x, c) => c.copy(mode = x) )
            .text("Application mode")
            .validate(x =>
                if (List("ide", "cluster").contains(x)) success
                else null)
        opt[Seq[String]]('d', "dimensions")
            .valueName("<table1>,<table2>...")
            .action( (x, c) => c.copy(tables = x) )
            .text("List of tables to process")
            .validate(x =>
                if (x.nonEmpty) success
                else null)
        opt[String]('s', "supplier")
            .action( (x, c) => c.copy(supplier = x) )
            .text("Supplier name")
            .validate(x =>
                if (x.nonEmpty) success
                else null)
        opt[String]('p', "provider")
            .action( (x, c) => c.copy(provider = x) )
            .text("Provider name")
            .validate(x =>
                if (x.nonEmpty) success
                else null)
        opt[String]('e', "env")
            .action( (x, c) => c.copy(env = x) )
            .text("Environment configuration")
            .validate(x =>
                if (x.nonEmpty) success
                else null)
        opt[String]('c', "country")
            .action( (x, c) => c.copy(country = x) )
            .text("Country Code")
            .validate(x =>
                if (x.nonEmpty) success
                else null)
    }

    parser.parse(args, Config()) match {
        case Some(config) =>
            cfg = config
        case None =>
            println("Parsing the arguments script Failed.")
    }

    /**
      * Get the job execution mode
      *
      * @return dev, preprod, prod
      */
    def getMode: String = this.cfg.mode

    /**
      * Get the name of the supplier
      *
      * @return the name of the supplier
      */
    def getSupplierName: String = this.cfg.supplier

    /**
      * Get the country code
      *
      * @return the country code
      */
    def getCountryCode: String = this.cfg.country

    /**
      * Get the name of the provider
      *
      * @return the name of the provider
      */
    def getProviderName: String = this.cfg.provider

    /**
      * Get the dimensions list
      *
      * @return the list of dimensions to process
      */
    def getTables: Seq[String] = this.cfg.tables
    
    def getEnvironment: String = this.cfg.env

}
