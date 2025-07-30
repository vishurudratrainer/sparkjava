import java.util.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;

public class BroadcastExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("IN", "US", "UK", "IN");
        Map<String, String> countryMap = new HashMap<>();
        countryMap.put("IN", "India");
        countryMap.put("US", "United States");
        countryMap.put("UK", "United Kingdom");

        Broadcast<Map<String, String>> broadcastVar = sc.broadcast(countryMap);
        JavaRDD<String> result = sc.parallelize(data).map(code -> broadcastVar.value().getOrDefault(code, "Unknown"));
        result.foreach(System.out::println);

        sc.close();
    }
}