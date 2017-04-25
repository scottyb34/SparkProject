



import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class NoaaDataExtractor {

	SparkSession spark;
	
	public NoaaDataExtractor(SparkSession sesh) {
		spark = sesh;
	}
	
	public Dataset<Row> extractData(String inputPath) throws AnalysisException {
	
		Dataset<Row> df = spark.read().csv(inputPath);

		return df;
	}
	
 }
