import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class JoinShuffleOptimizationExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("JoinShuffleOptimizationExample")
                .master("local[*]")
                // Enable Adaptive Query Execution (AQE) - highly recommended for Spark 3.x+
                // AQE dynamically optimizes queries during runtime, including join strategies and shuffle partitions.
                .config("spark.sql.adaptive.enabled", "true")
                // Threshold for Broadcast Join (default is 10MB). Increase if you have small lookup tables.
                .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
                // Initial number of shuffle partitions. AQE can adjust this dynamically.
                .config("spark.sql.shuffle.partitions", "200")
                .getOrCreate();

        // Create two sample DataFrames
        Dataset<Row> users = spark.createDataFrame(java.util.Arrays.asList(
                new User(1, "Alice"),
                new User(2, "Bob"),
                new User(3, "Charlie")
        ), User.class);

        Dataset<Row> orders = spark.createDataFrame(java.util.Arrays.asList(
                new Order(101, 1, "Laptop"),
                new Order(102, 2, "Mouse"),
                new Order(103, 1, "Keyboard"),
                new Order(104, 3, "Monitor")
        ), Order.class);

        // 1. Broadcast Joins: If one DataFrame is small enough to fit into executor memory,
        // broadcast it to all executors to avoid shuffling the large DataFrame.
        // Spark will automatically do this if `spark.sql.autoBroadcastJoinThreshold` is met.
        // You can also explicitly hint it:
        Dataset<Row> broadcastJoinResult = users.join(functions.broadcast(orders), users.col("id").equalTo(orders.col("userId")));
        System.out.println("Broadcast Join Result:");
        broadcastJoinResult.show();

        // 2. Skewed Joins: If a join key has highly skewed values, it can lead to performance bottlenecks.
        // AQE (enabled above) helps mitigate this by re-partitioning skewed partitions.
        // For Spark 2.x or specific cases, manual techniques like salting keys might be needed.

        // 3. Re-partitioning: Sometimes, re-partitioning a DataFrame before a join or aggregation
        // can help ensure an even distribution of data.
        // If you know you'll be joining on 'id', you can re-partition 'users' by 'id'.
        // This should be used cautiously as re-partitioning itself is a shuffle.
        Dataset<Row> repartitionedUsers = users.repartition(functions.col("id"));
        // Then join repartitionedUsers with orders.

        // 4. Reduce Shuffle Partitions: 'spark.sql.shuffle.partitions'
        // Setting this too high creates many small files and high overhead.
        // Setting it too low can lead to large partitions and OOM errors.
        // AQE helps here, but a sensible default is important.

        spark.stop();
    }

    // Helper classes for creating DataFrames
    public static class User implements java.io.Serializable {
        private int id;
        private String name;

        public User(int id, String name) { this.id = id; this.name = name; }
        public int getId() { return id; }
        public String getName() { return name; }
        public void setId(int id) { this.id = id; }
        public void setName(String name) { this.name = name; }
    }

    public static class Order implements java.io.Serializable {
        private int orderId;
        private int userId;
        private String item;

        public Order(int orderId, int userId, String item) { this.orderId = orderId; this.userId = userId; this.item = item; }
        public int getOrderId() { return orderId; }
        public int getUserId() { return userId; }
        public String getItem() { return item; }
        public void setOrderId(int orderId) { this.orderId = orderId; }
        public void setUserId(int userId) { this.userId = userId; }
        public void setItem(String item) { this.item = item; }
    }
}