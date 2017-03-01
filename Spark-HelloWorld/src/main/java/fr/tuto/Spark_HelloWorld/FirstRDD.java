package fr.tuto.Spark_HelloWorld;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FirstRDD {
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
 
        String path = Paths.get(FirstRDD.class.getResource("/ratings.txt").getPath()).toString();
        JavaRDD<String> lines = sc.textFile(path.toString());
        System.out.println("Lines count: " + lines.count());
    }
}