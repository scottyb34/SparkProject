
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;



public class WeatherDataExtractor {
	public static Map<String, String> wbanConversion;

	public static void main(String[] args) throws Exception {
		wbanConversion = new HashMap<String, String>();
		wbanConversion.put("13881", "Texas");
		wbanConversion.put("94870", "Minnesota");
		wbanConversion.put("03149", "Colorado");
		wbanConversion.put("23054", "New York");
		wbanConversion.put("94811", "Pitt");
		wbanConversion.put("04805", "Cleveage");
		wbanConversion.put("94823", "Madis");

		
		System.out.println(System.getProperty("hadoop.home.dir"));

		String inputPath = args[0];
		String outputPath = args[1];

		FileUtils.deleteQuietly(new File(outputPath));

		SparkConf conf = new SparkConf().setAppName("word-counter").setMaster("local").set("spark.cores.max", "10");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//pulls in file
		JavaRDD<String> rdd = sc.textFile(inputPath);
		
		//filter predicate
		//Function<String, Boolean> filterPredicate = e -> e.substring(0, 5).matches("13881|94870|03149|23054|94811|04805|94823");

		//List<Integer> ind =(List<Integer>) Arrays.asList(new Integer[]{6,10,28,30,40});
		
		
		
		
		
		JavaRDD<String> wban = rdd.filter(e -> e.substring(0, 5).matches("13881|94870|03149|23054|94811|04805|94823"))
				.map(e -> (String) e.replaceFirst("^.....", wbanConversion.get(e.substring(0, 5))))
				.map(e -> Arrays.asList(e.split(",")))
				.map(e -> IntStream.range(0, e.size())
						.filter(i -> Arrays.asList(new Integer[]{0,6,10,28,30,40}).contains(new Integer(i)))
						.mapToObj(e::get).map(e2 -> e2.toString())
						.collect(Collectors.joining(",")));
		
		
		
//		JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
//				.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
//
//		List<Tuple2<String, Integer>> finalCounts = counts.filter((x) -> x._1().contains("@")).collect();
//
//		for (Tuple2<String, Integer> count : finalCounts)
//			System.out.println(count._1() + " " + count._2());
//
//		counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
//				.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
//
//		counts = counts.filter((x) -> x._2() > 20);
//
//		long time = System.currentTimeMillis();
//		long countEntries = counts.count();
//		System.out.println(countEntries + ": " + String.valueOf(System.currentTimeMillis() - time));

//		counts.saveAsTextFile(outputPath);
		wban.saveAsTextFile(outputPath);
		sc.close();

	}

}
