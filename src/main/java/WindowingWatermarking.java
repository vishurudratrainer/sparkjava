import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;

import java.util.concurrent.TimeoutException;

public class WindowingWatermarking {
    public static void main(String[] args) throws StreamingQueryException, InterruptedException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("WindowingWatermarking")
                .master("local[*]")
                .config("spark.sql.streaming.schemaInference", "true") // Allow schema inference for simpler JSON/CSV
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // We'll use a rate source for simplicity, which generates data with a timestamp.
        // In a real scenario, this would be Kafka, files, etc., where you parse an event-time.
        Dataset<Row> inputStream = spark.readStream()
                .format("rate")        // Generates data (timestamp, value) at a specified rate
                .option("rowsPerSecond", 1) // Generate 1 row per second
                .option("rampUpTime", 0)   // Start immediately
                .load();

        // Assume our "value" column contains some category, and "timestamp" is the event time.
        // For 'rate' source, 'value' is just an increasing number. Let's create a dummy category.
        Dataset<Row> events = inputStream
                .withColumn("category", functions.expr("CASE WHEN value % 2 = 0 THEN 'even' ELSE 'odd' END"))
                .withColumnRenamed("timestamp", "event_time"); // Rename for clarity

        // 1. Define a Watermark:
        // This specifies the threshold for late data. Events arriving up to 10 seconds late will be processed.
        // Data older than event_time - 10 seconds will be dropped.
        // The watermark column must be a timestamp.
        Dataset<Row> withWatermark = events.withWatermark("event_time", "10 seconds");

        // 2. Perform a Windowed Aggregation:
        // Group by a time window (e.g., 5-second sliding window) and the category.
        // The window starts every 5 seconds and has a duration of 10 seconds.
        Dataset<Row> windowedCounts = withWatermark
                .groupBy(
                        functions.window(functions.col("event_time"), "10 seconds", "5 seconds"), // Tumbling window of 10s, slides every 5s
                        functions.col("category")
                )
                .count()
                .orderBy("window.start", "category"); // Order for better console output

        // Write the results to console. 'complete' mode is necessary for windowed aggregations.
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete") // Required for windowed aggregations when watermark is used
                .format("console")
                .option("truncate", "false")
                .start();

        System.out.println("Processing events with 10-second window and 10-second watermark.");
        System.out.println("Observe how counts change and old windows might be finalized.");
        System.out.println("Press Ctrl+C to stop the application.");

        query.awaitTermination();
    }
}

/**
 * You'll observe the window column showing start and end timestamps, and the count for each category within that window. As time progresses, new windows will appear, and older windows will eventually be "finalized" and stop updating due to the watermark.
 */