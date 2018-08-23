#!/usr/bin/env bash
/home/$USER/bin/spark-2.1.0-bin-hadoop2.7/bin/spark-submit \
  --class sparkworkshop.SparkRunner \
  --master spark://lbaran:6066 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 4G \
  --total-executor-cores 2 \
  target/spark-workshop-1.0.0-jar-with-dependencies.jar  \
  sparkworkshop.examples.UDFFunctionExample
