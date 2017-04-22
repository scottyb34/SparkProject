/*
 * 1. Reads in JSON Yelp Business and Review files 
 * 2. Create DataFrame objects
 * 3. Spark SQL queries to pull attributes of interest
 */


import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class YelpParser {
	
	
	public static void main(String[] args) {
		String yelpBusinessInputPath = args[0];
		String yelpReviewInputPath = args[1];
		String outputPath = args[2];

		FileUtils.deleteQuietly(new File(outputPath));

		SparkSession spark = SparkSession
				  .builder()
				  .appName("YelpDataParser")
				  .getOrCreate();
		
		// Creates DataFrame object
		Dataset<Row> businessDataFrame = spark.read().json(yelpBusinessInputPath);
		
		//Create a table to run SQL queries 
		businessDataFrame.createOrReplaceTempView("businessTable");
		
		//Pull business id and state to create new DataFrame
		Dataset<Row> businessSQLDF = spark.sql("SELECT business_id, state FROM businessTable");
		
		
		Dataset<Row> reviewDataFrame = spark.read().json(yelpReviewInputPath);
		
		reviewDataFrame.createOrReplaceTempView("reviewTable");
		
		Dataset<Row> reviewSQLDF = spark.sql("SELECT business_id, state, stars FROM reviewTable");
	
		
	}
}
