import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class FirstRDD {

	private static final Logger LOGGER = LoggerFactory.getLogger(FirstRDD.class);

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Please provide the path of input file as first parameter");
			return;
		}

		String inputFilePath = args[0];

		SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(inputFilePath);
		LOGGER.info("Lines count: " + lines.count());

		lines.flatMap(text -> Arrays.asList(text.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a, b) -> a + b)
				.foreach(result -> LOGGER.info(String.format("Word [%s] count [%d].", result._1(), result._2)));
	}

}