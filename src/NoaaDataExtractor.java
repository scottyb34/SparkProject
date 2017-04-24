



import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class NoaaDataExtractor {

	SparkSession spark;
	
	public NoaaDataExtractor(SparkSession sesh) {
		spark = sesh;
	}
	
	public void extractData(String inputPath) throws AnalysisException {
	
		Dataset<Row> df = spark.read().csv(inputPath);
		
		df.createTempView("allDataTable");

		//Dataset<Row> desiredCol = spark.sql("SELECT _c0, _c1, _c6, _c10, _c27 from allDataTable");
		
		df.show();
	}
	
 }
