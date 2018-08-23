# Spark ETL

## How to launch the job locally
Launch sbt with enough memory to launch Spark job

		sbt -mem 5000 -Dsbt.override.build.repos=true -Dsbt.repository.config=deployment/artifactory.repositories
		
Launch Spark job with this command

		sbt:ETL> run --mode ide --dimensions Contact --supplier PDGI --country DE --provider INFOLINK --env <conf_file_path>

## How to start Spark cluster on local machine
      cd spark/sbin
      ./start-master.sh
      ./start-slave.sh 127.0.0.1:7077
      ./start-history-server.sh --properties-file /Users/gpayen/Documents/spark_2.2.1/conf/history.properties

## How to launch the job with Spark Standalone cluster
If you did not package your job with assembly, you may have to copy jar dependencies to Spark's jar folder 
- config-1.2.1.jar
- scopt_2.11-3.7.0.jar
- scala-logging-slf4j_2.11-2.1.2.jar
- scala-logging-api_2.11-2.1.1.jar
- ojdbc6.jar
- rdf-framework-spark-1.0.48.2.jar
- rdf-framework-utils-1.0.48.2.jar

Then, you can submit job like this:

Local mode

	./spark-submit ../../ETL/target/scala-2.11/etl_2.11-1.0.jar --mode ide --dimensions Contact --supplier PDGI --country DE --provider INFOLINK  --env <conf_file_path>
	
Local mode, like in _rdf-business-rule-engine_ project

    ./spark-submit --class com.imshealth.rdf.framework.spark.jobs.runner.IntelliJRunner ../../ETL/target/scala-2.11/etl_2.11-1.0.jar framework.EntryPoint --dimensions Contact --supplier PDGI --country DE --provider INFOLINK --env <conf_file_path>
    
Cluster mode (start master and at least one worker before submitting the job)

	./spark-submit --properties-file /Users/gpayen/Documents/spark_2.2.1/conf/etl.properties ../../ETL/target/scala-2.11/etl_2.11-1.0.jar --mode cluster --dimensions Contact --supplier PDGI --country DE --provider INFOLINK --env <conf_file_path>

Cluster mode, like in _rdf-business-rule-engine_ project (start master and at least one worker before submitting the job)

	./spark-submit --properties-file /Users/gpayen/Documents/spark_2.2.1/conf/etl.properties --class com.imshealth.rdf.framework.spark.jobs.runner.ClusterRunner ../../ETL/target/scala-2.11/etl_2.11-1.0.jar framework.EntryPoint --dimensions Contact --supplier PDGI --country DE --provider INFOLINK --env <conf_file_path>
	
## Scaladoc
Documentation of the classes was compiled and is also versioned. To access it, use your favorite web brower, and reach _index.html_ file here: _target/scala-2.11/api/index.html_