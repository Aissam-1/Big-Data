package rdf.spark.etl

import com.imshealth.rdf.framework.spark.jobs.runner.{ClusterRunner, IntelliJRunner}
import rdf.spark.etl.framework.lib.{CommandLines, ResourcesLauncher, Session}

/**
  * Represents starting class of the application (extends App)
  * Its purpose is to fetch command lines arguments, and launch Spark RDF Framework
  *
  * @author Guillaume Payen
  * @version 1.0
  */
object Application extends App{
	
	val cmd = new CommandLines(args, new ResourcesLauncher("defaults"))
	Session.instance.init(cmd)
	
	System.setProperty("APPLICATION_CONF_LOCATION", cmd.getEnvironment)
	
	cmd.getMode match {
		case "ide" => IntelliJRunner.main("rdf.spark.etl.framework.EntryPoint" +: args)
		case "cluster" => ClusterRunner.main("rdf.spark.etl.framework.EntryPoint" +: args)
	}
	
}
