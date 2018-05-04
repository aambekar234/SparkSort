import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public class SparkSort 
{
	public static void main(String[] args) 
	{
		
		Long start_t = System.currentTimeMillis();
		JavaSparkContext sc = new JavaSparkContext("local[*]", "SparkSort");	
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		PairFunction<String, String, String> keyvaluepair = new PairFunction<String, String, String>() {
									public Tuple2<String, String> call(String line) {
									
									return new Tuple2<String, String>(line, ""); }};
		
		JavaPairRDD<String, String> sortedOutput = lines.mapToPair(keyvaluepair).sortByKey(true);
		sortedOutput.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

			public Iterator<String> call(Tuple2<String, String> token) throws Exception {
				List<String> tmp = new ArrayList<String>();
				tmp.add(token._1() +"\r");
				return tmp.iterator();
			}
		}).saveAsTextFile(args[1]);
		long timetook = (System.currentTimeMillis() - start_t)/1000;
		System.out.println("***Time took to sort is " + timetook+" seconds.***");
	}
}
