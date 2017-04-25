
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class WeatherDataExtractor{

	public static Map<String, String> wbanConversion;

	public void extract(String input, SparkSession spark) throws Exception {
		wbanConversion = new HashMap<String, String>();
		wbanConversion.put("13881", "NC");
		wbanConversion.put("94870", "IL");
		wbanConversion.put("23183", "AZ");
		wbanConversion.put("23169", "NV");
		wbanConversion.put("14837", "WI");
		wbanConversion.put("14820", "OH");
		wbanConversion.put("94823", "PA");

		System.out.println(System.getProperty("hadoop.home.dir"));

		String inputPath = input;
		String outputPath = "/noaaOut";

		FileUtils.deleteQuietly(new File(outputPath));

		// pulls in file

		JavaRDD<String> rdd = spark.sparkContext().textFile(inputPath, 10).toJavaRDD();

		// filter predicate
		// Function<String, Boolean> filterPredicate = e -> e.substring(0,
		// 5).matches("13881|94870|03149|23054|94811|04805|94823");

		// List<Integer> ind =(List<Integer>) Arrays.asList(new
		// Integer[]{6,10,28,30,40});
		Integer[] first = new Integer[] { 0, 1, 4, 6, 13, 14, 19 };

		Integer[] second = new Integer[] { 0, 1, 6, 10, 28, 30, 40 };


		JavaRDD<String> wban = rdd.filter(e -> e.substring(0, 5).matches("13881|94870|23183|23169|14837|14820|94823"))
				.map(e -> (String) e.replaceFirst("^.....", wbanConversion.get(e.substring(0, 5))))
				.map(e -> Arrays.asList(e.split(",")))
				.map(e -> IntStream.range(0, e.size())
				.filter(i -> Arrays.asList(e.size() > 30 ? second : first).contains(new Integer(i)))
				.mapToObj(e::get).map(e2 -> e2.toString().matches("[0-9]{8}") ? (e2.toString().substring(0, 4) + "-" + e2.toString().substring(4, 6) + "-" + e2.toString().substring(6, 8)) : e2.toString())
				.collect(Collectors.joining(",")));


		wban.saveAsTextFile(outputPath);

	}
	

}
