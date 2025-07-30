import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class KafkaSparkIntegration {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaSparkIntegration")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        String kafkaBrokers = "localhost:9092"; // Your Kafka broker(s) address
        String inputTopic = "spark_input_topic";
        String outputTopic = "spark_output_topic";
        String checkpointDirectory = "data/checkpoint/kafka_stream";

        // Define schema for the JSON data we expect from Kafka message values
        StructType messageSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("event_type", DataTypes.StringType, true),
                DataTypes.createStructField("timestamp", DataTypes.TimestampType, true)
        });

        // 1. Read from Kafka
        // The 'subscribe' option specifies the topic(s) to consume.
        // 'startingOffsets' can be 'earliest' or 'latest'.
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBrokers)
                .option("subscribe", inputTopic)
                .option("startingOffsets", "latest") // Start from the latest offset
                .load();

        // 2. Process the Kafka messages
        // Kafka messages are read as (key, value, topic, partition, offset, timestamp, timestampType).
        // The 'value' column is binary. We need to cast it to String and then parse JSON.
        Dataset<Row> parsedStream = kafkaStream
                .selectExpr("CAST(value AS STRING) as json") // Cast binary value to String
                .select(functions.from_json(functions.col("json"), messageSchema).as("data")) // Parse JSON string
                .select("data.*"); // Flatten the struct to get individual fields

        // Example Transformation: Count events by type
        Dataset<Row> eventCounts = parsedStream
                .groupBy("event_type")
                .count();

        // 3. Write results back to Kafka (or another sink)
        // Convert the DataFrame back to a format suitable for Kafka (key/value bytes).
        // Here, we'll write the event_type as key and count as value (both as strings).
        StreamingQuery query = eventCounts.select(
                        functions.col("event_type").cast("string").as("key"), // Cast key to string
                        functions.col("count").cast("string").as("value")    // Cast value to string
                )
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBrokers)
                .option("topic", outputTopic)
                .option("checkpointLocation", checkpointDirectory)
                .outputMode("update") // Use update mode for aggregations if you want to see changes
                .start();

        System.out.println("Spark Structured Streaming integrated with Kafka.");
        System.out.println("Reading from topic: " + inputTopic);
        System.out.println("Writing to topic: " + outputTopic);
        System.out.println("To produce messages: kafka-console-producer --broker-list localhost:9092 --topic " + inputTopic);
        System.out.println("Example message: {\"id\":1, \"event_type\":\"click\", \"timestamp\":\"2025-07-27T10:00:00Z\"}");
        System.out.println("To consume output: kafka-console-consumer --bootstrap-server localhost:9092 --topic " + outputTopic + " --from-beginning --property print.key=true");
        System.out.println("Press Ctrl+C to stop the application.");

        query.awaitTermination();
    }
}