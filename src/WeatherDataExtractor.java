
import java.io.File;
import java.io.Serializable;
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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class WeatherDataExtractor implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static Map<String, String> wbanConversion;
	
	private static JavaRDD<String> wban;
	private Dataset<Row> wbanDF;
	private SparkSession spark;
	
	
	public WeatherDataExtractor(SparkSession sparkSesh) { spark = sparkSesh; }
	
	public JavaRDD<String> getWBAN() { return wban; }
	public Dataset<Row> getWBAN_DF() { return wbanDF; }

	public void parseWBAN(String inPath) throws Exception {
		wbanConversion = new HashMap<String, String>();
		wbanConversion.put("13881", "NC");  //Charlotte
		wbanConversion.put("94870", "IL");  //Urban-Popping-Champagne
		wbanConversion.put("03149", "AZ");  //Phoenix
		wbanConversion.put("23054", "NV");  //Las Vegas
		wbanConversion.put("94811", "WI");  //Madison
		wbanConversion.put("04805", "OH");  //Cleveland 
		wbanConversion.put("94823", "PA");  //Pittsburgh

		System.out.println(System.getProperty("hadoop.home.dir"));

		String inputPath = inPath;

		//SparkConf conf = new SparkConf().setAppName("WX-Parser").setMaster("local").set("spark.cores.max", "10");
		//JavaSparkContext sc = new JavaSparkContext(conf);

		// pulls in file
		
		JavaRDD<String> rdd = spark.sparkContext().textFile(inputPath, 10).toJavaRDD();

		// filter predicate
		// Function<String, Boolean> filterPredicate = e -> e.substring(0,
		// 5).matches("13881|94870|03149|23054|94811|04805|94823");

		// List<Integer> ind =(List<Integer>) Arrays.asList(new
		// Integer[]{6,10,28,30,40});

		wban = rdd.filter(e -> e.substring(0, 5).matches("13881|94870|03149|23054|94811|04805|94823"))
				.map(e -> (String) e.replaceFirst("^.....", wbanConversion.get(e.substring(0, 5))))
				.map(e -> Arrays.asList(e.split(",")))
				.map(e -> IntStream.range(0, e.size())
						.filter(i -> Arrays.asList(new Integer[] { 0, 6, 10, 28, 30, 40 }).contains(new Integer(i)))
						.mapToObj(e::get).map(e2 -> e2.toString()).collect(Collectors.joining(",")));

		// JavaPairRDD<String, Integer> counts = rdd.flatMap(x ->
		// Arrays.asList(x.split(" ")).iterator())
		// .mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y)
		// -> x + y);
		//
		// List<Tuple2<String, Integer>> finalCounts = counts.filter((x) ->
		// x._1().contains("@")).collect();
		//
		// for (Tuple2<String, Integer> count : finalCounts)
		// System.out.println(count._1() + " " + count._2());
		//
		// counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
		// .mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y)
		// -> x + y);
		//
		// counts = counts.filter((x) -> x._2() > 20);
		//
		// long time = System.currentTimeMillis();
		// long countEntries = counts.count();
		// System.out.println(countEntries + ": " +
		// String.valueOf(System.currentTimeMillis() - time));

		// counts.saveAsTextFile(outputPath);
		
		//wban.saveAsTextFile(outputPath);
		
		createSchema();

	}
	
	private void createSchema() {
		
		// Seven attributes needed for NOAA Dataframe / Dataset object 
		String schemaString = "weatherState weatherDate tempAvg dewPt snow precTotal wind";
		
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		
		StructType schema = DataTypes.createStructType(fields);
		
		SQLContext sqlContext = new SQLContext(spark.sparkContext());
		
		JavaRDD<Row> wbanRowRDD = wban.map(new Function<String, Row>() {
			  @Override
			  public Row call(String record) throws Exception {
				String date = "";
			    String[] attributes = record.split(",");
			    if (attributes.length < 7) return RowFactory.create("");
			    if (attributes[1].length() >= 7)
			    	date = attributes[1].substring(0, 4) + "-" + attributes[1].substring(4, 6) + "-" + attributes[1].substring(6, 8);
			    return RowFactory.create(attributes[0], date, attributes[2], attributes[3], attributes[4], attributes[5], attributes[6]);
			  }
			});
		
		
		wbanDF = spark.createDataFrame(wbanRowRDD, schema);
		
	}

}
