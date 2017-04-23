import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainClass {

	/* 
	 * 	1st argument: yelp business json data path
	 *  2nd argument: yelp reviews json data path
	 *  3rd argument: noaa data path
	 */
	public static void main(String[] args) throws Exception {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Yelp Weather data")
				  .getOrCreate();
		/*
		YelpParser yelper = new YelpParser(spark);
		
		yelper.parseYelpDataGet(args[0], args[1]);
		Dataset<Row> joinedYelpDataFrame = yelper.getJoinedDF();
		*/
		WeatherDataExtractor weatherExtract = new WeatherDataExtractor(spark);
		
		weatherExtract.parseWBAN(args[0]);
		Dataset<Row> wbanDF = weatherExtract.getWBAN_DF();
		
		wbanDF.show();
		
		//joinedYelpDataFrame.show();
		
	}
}
