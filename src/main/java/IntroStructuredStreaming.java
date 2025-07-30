import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeoutException;


public class IntroStructuredStreaming {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Create a SparkSession, which is the entry point for Spark SQL and Structured Streaming.
        SparkSession spark = SparkSession.builder()
                .appName("IntroStructuredStreaming")
                .master("local[*]") // Run Spark locally with all available cores.
                .getOrCreate();

        // Set log level to WARN to reduce verbosity for cleaner output.
        spark.sparkContext().setLogLevel("WARN");

        // Define a schema for the incoming data if reading from a file source
        // For socket source, Spark can infer schema for simple cases (like value as string).
        // Let's create a dummy schema for demonstration if we were reading JSON/CSV
        StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true)
        });

        // 1. Reading from a socket source (a common "hello world" for streaming)
        // This creates a DataFrame that represents the stream of lines from the listening socket.
        Dataset<Row> lines = spark.readStream()
                .format("socket") // Specifies the source format.
                .option("host", "localhost") // Host to listen on.
                .option("port", 9999)      // Port to listen on.
                .load();

        // 2. Perform a simple transformation: Count words in the incoming lines.
        // This demonstrates how you use standard DataFrame/Dataset operations on a streaming DataFrame.
        Dataset<Row> words = lines.selectExpr("explode(split(value, ' ')) as word");
        Dataset<Row> wordCounts = words.groupBy("word").count();

        // 3. Writing to a console sink (for debugging/demonstration purposes)
        // The `format("console")` writes the output of each micro-batch to the console.
        // `outputMode("complete")` means all the results of the aggregation will be re-computed and written.
        // Other output modes: "append" (only new rows), "update" (only updated rows).
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete") // Important for aggregations to see cumulative counts.
                .format("console")      // Print results to the console.
                .option("truncate", "false") // Don't truncate output columns.
                .start(); // Start the streaming query.


        System.out.println("Listening for data on localhost:9999. Type something in a terminal: nc -lk 9999");
        System.out.println("Press Ctrl+C to stop the application.");

        // Wait for the termination of the query. This prevents the main thread from exiting.
        query.awaitTermination();
    }
}