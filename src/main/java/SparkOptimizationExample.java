import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkOptimizationExample {
    public static void main(String[] args) {
        // Create a SparkSession, the entry point to programming Spark with the Dataset and DataFrame API.
        SparkSession spark = SparkSession.builder()
                .appName("SparkOptimizationExample")
                .master("local[*]") // Run Spark locally with as many worker threads as logical cores.
                .getOrCreate();

        // Catalyst Optimizer in action: Spark will optimize this chain of operations.
        // It will reorder filters, push down projections, etc., for efficiency.
        Dataset<Row> people = spark.read().json("src/main/resources/people.json"); // Assume people.json exists

        Dataset<Row> youngAdults = people.filter(people.col("age").geq(18)) // Filter for age >= 18
                .filter(people.col("age").leq(30))  // And age <= 30
                .select("name", "age");             // Select only name and age

        youngAdults.show();

        // Tungsten Engine benefits:
        // Tungsten optimizes memory layout and CPU efficiency for operations like aggregations, joins, and shuffles.
        // While not directly visible in simple API calls, it's underlying the performance of operations like groupBy.
        Dataset<Row> ageCounts = people.groupBy("age").count(); // Grouping benefits from Tungsten's optimizations

        ageCounts.show();

        spark.stop();
    }
}