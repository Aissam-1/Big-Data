SPARK:
------
* Quand on lance en "Local Mode", on n'a pas besoin d'installer ni de lancer spark, Intellij va
  le récupérer via Maven et lancer le master en local
* Quand on lance en "Cluster Mode", là on configure notre code pour pointer sur le server master du cluster spark
  (je peux utiliser celui sur ubuntu)



./spark-submit --driver-java-options "-DAPPLICATION_CONF_LOCATION=/c/Users/ihamaoui/Desktop/Tools/spark_workspace/rdf-spark-etl/src/main/resources/dev.properties" --properties-file /c/Users/ihamaoui/Desktop/Tools/spark-2.2.1-bin-hadoop2.7/conf/etl.properties --class com.imshealth.rdf.framework.spark.jobs.runner.ClusterRunner /c/Users/ihamaoui/Desktop/Tools/spark_workspace/rdf-spark-etl/target/scala-2.11/ETL-assembly-1.0.jar framework.EntryPoint --dimensions Contact --supplier PDGI --country DE --provider INFOLINK



SBT Doc:
--------
Faire un clean:
	sbt clean

Compiler:
	sbt compile

Exécuter l'app^:
	sbt run

	
Packager (générer le .jar sans les dépendances)
	sbt package

Assembler (générer le .jar avec les dépendances)
	sbt assembly

Skipper les tests
	sbt "set test in assembly := {}" clean assembly


Quelques Commandes:
-------------------
* sbt settings -V
* sbt settings
	- sbt show name
	- sbt set name := "MyFirstProject"
	- sbt show libraryDependencies
	- sbt set libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"
	- ...
* sbt task