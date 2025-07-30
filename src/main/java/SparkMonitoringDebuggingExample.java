import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkMonitoringDebuggingExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkMonitoringDebuggingExample")
                .master("local[*]")
                // Enable the Spark UI for monitoring. By default, it runs on port 4040.
                // You can access it at http://localhost:4040
                .config("spark.ui.port", "4040")
                // Enable event logging for history server (useful for post-mortem analysis)
                // Ensure spark.eventLog.dir is writable and accessible.
                // .config("spark.eventLog.enabled", "true")
                // .config("spark.eventLog.dir", "/tmp/spark-events")
                .getOrCreate();

        System.out.println("Spark UI accessible at http://localhost:4040");

        // Simulate some data processing
        Dataset<Row> data = spark.range(0, 10_000_000).toDF("value");

        // Perform a transformation and action
        Dataset<Row> processedData = data.filter(data.col("value").mod(100).equalTo(0));

        // Trigger an action to execute the lineage
        long count = processedData.count();
        System.out.println("Count of divisible by 100: " + count);

        // Simulate an error for debugging demonstration
        try {
            // This will cause a NullPointerException
            spark.createDataset(java.util.Arrays.asList("a", null, "c"), org.apache.spark.sql.Encoders.STRING())
                    .filter(col("value").startsWith("a"))
                    .show();
        } catch (Exception e) {
            System.err.println("\n--- SIMULATED ERROR FOR DEBUGGING ---");
            e.printStackTrace(); // Look at the stack trace for the root cause
            System.err.println("--- END SIMULATED ERROR ---");
            // In a real application, you might log this more robustly
        }

        // Keep the application running for a bit so you can inspect the Spark UI
        System.out.println("\nApplication running. Check Spark UI at http://localhost:4040. Press Enter to stop...");
        try {
            System.in.read(); // Wait for user input
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }

        spark.stop();
        System.out.println("Spark application stopped.");
    }
}