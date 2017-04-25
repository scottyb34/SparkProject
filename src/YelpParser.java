/*
 * 1. Reads in JSON Yelp Business and Review files 
 * 2. Create DataFrame objects
 * 3. Spark SQL queries to pull attributes of interest
 */


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class YelpParser {
	
	private Dataset<Row> joinedDF;
	private SparkSession spark;
	
	public YelpParser(SparkSession sparkSesh) { spark = sparkSesh; }
	
	public Dataset<Row> getJoinedDF() { return joinedDF; }
	
	public void parseYelpDataGet(String yelpBusinessPath, String yelpReviewPath) {
		String yelpBusinessInputPath = yelpBusinessPath;
		String yelpReviewInputPath = yelpReviewPath;
		
		// Creates DataFrame object
		Dataset<Row> businessDataFrame = spark.read().json(yelpBusinessInputPath);
		
		//Create a table to run SQL queries 
		businessDataFrame.createOrReplaceTempView("businessTable");
		
		//Pull business id and state to create new DataFrame
		Dataset<Row> businessSQLDF = spark.sql("SELECT business_id, state, stars as avgstars FROM businessTable");
		
		
		Dataset<Row> reviewDataFrame = spark.read().json(yelpReviewInputPath);
		
		reviewDataFrame.createOrReplaceTempView("reviewTable");
		
		Dataset<Row> reviewSQLDF = spark.sql("SELECT business_id, date, stars FROM reviewTable");
		
		businessSQLDF.createOrReplaceTempView("filteredBusinessTable");

		reviewSQLDF.createOrReplaceTempView("filteredReviewTable");
		
		joinedDF = spark.sql("select filteredReviewTable.business_id, date, stars, state, avgstars from filteredReviewTable left "
				+ "join filteredBusinessTable on filteredBusinessTable.business_id = filteredReviewTable.business_id");

		
	}
}
