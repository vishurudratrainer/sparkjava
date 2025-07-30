import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeoutException;

/**
 * Processing Real-Time Data Streams
 * This example demonstrates processing a stream of simple JSON data from a directory, simulating a real-time data ingestion scenario where files are continuously dropped.
 */
public class RealTimeDataProcessing {
    public static void main(String[] args) throws StreamingQueryException, InterruptedException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("RealTimeDataProcessing")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        String inputDirectory = "data/input"; // Directory to monitor for new files
        String checkpointDirectory = "data/checkpoint/json_stream"; // Checkpoint directory for state management

        // Ensure input directory exists and is clean
        try {
            Files.createDirectories(Paths.get(inputDirectory));
            Files.createDirectories(Paths.get(checkpointDirectory));
            // Clean up previous checkpoint data for fresh run
            Files.walk(Paths.get(checkpointDirectory))
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(java.io.File::delete);
        } catch (IOException e) {
            System.err.println("Could not create/clean directories: " + e.getMessage());
            return;
        }

        // Define schema for the incoming JSON data
        StructType userSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        });

        // Read from file stream (JSON files)
        Dataset<Row> inputStream = spark.readStream()
                .schema(userSchema) // Apply the defined schema
                .option("maxFilesPerTrigger", 1) // Process one file at a time
                .json(inputDirectory); // Monitor this directory for new JSON files

        // Transformation: Filter users older than 25
        Dataset<Row> filteredUsers = inputStream.filter(functions.col("age").gt(25));

        // Write to console sink
        StreamingQuery query = filteredUsers.writeStream()
                .outputMode("append") // Append mode as we are only adding new rows
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", checkpointDirectory) // Crucial for fault tolerance and state recovery
                .start();

        System.out.println("Monitoring directory: " + inputDirectory + " for new JSON files.");
        System.out.println("Generate files using the `generateData` method below or manually.");
        System.out.println("Press Ctrl+C to stop the application.");

        // Simulate new files arriving periodically
        // You can run this in a separate thread or just generate files manually
        Thread dataGenerator = new Thread(() -> {
            try {
                int fileCount = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    String fileName = inputDirectory + "/data_" + (fileCount++) + ".json";
                    String data = "{\"name\":\"User" + fileCount + "\", \"age\":" + (20 + fileCount) % 40 + "}\n" +
                            "{\"name\":\"OldUser" + fileCount + "\", \"age\":" + (30 + fileCount) % 50 + "}";
                    Files.write(Paths.get(fileName), data.getBytes(), StandardOpenOption.CREATE);
                    System.out.println("Generated file: " + fileName);
                    Thread.sleep(5000); // Generate a new file every 5 seconds
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("Data generator interrupted: " + e.getMessage());
            }
        });
        dataGenerator.setDaemon(true); // Allow the program to exit if main thread finishes
        dataGenerator.start();

        query.awaitTermination(); // Wait for the streaming query to terminate
        dataGenerator.interrupt(); // Stop the data generator thread
    }
}

/**
 * spark-submit --class com.sparkexamples.RealTimeDataProcessing \
 *              --master local[*] \
 *              target/YourSparkProject-1.0-SNAPSHOT.jar
 * The application will start generating data_*.json files in the data/input directory every 5 seconds, and Spark will process them.
 */