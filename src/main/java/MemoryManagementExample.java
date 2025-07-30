import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class MemoryManagementExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("MemoryManagementExample")
                .master("local[*]")
                // Configure Spark memory. spark.executor.memory is critical.
                // spark.memory.fraction controls the proportion of JVM heap space used for Spark data.
                .config("spark.executor.memory", "4g") // Example: Allocate 4GB to each executor
                .config("spark.memory.fraction", "0.6") // 60% of executor memory for Spark data
                .config("spark.sql.shuffle.partitions", "200") // Reduce if too many partitions for your data size
                .getOrCreate();

        // 1. Caching/Persisting Data: Use `cache()` or `persist()` for frequently accessed RDDs/DataFrames.
        // This keeps data in memory (or on disk), avoiding recomputation.
        Dataset<Row> largeDataset = spark.range(0, 10_000_000).toDF("id"); // Create a large DataFrame

        // Cache the dataset in memory. StorageLevel.MEMORY_AND_DISK_SER is often a good default.
        // MEMORY_ONLY: Faster, but recomputes on eviction.
        // MEMORY_AND_DISK: Spills to disk if not enough memory.
        // MEMORY_ONLY_SER: Serialized, saves space, but more CPU to (de)serialize.
        largeDataset.persist(StorageLevel.MEMORY_AND_DISK_SER());

        // Perform some operations that will benefit from caching
        long count1 = largeDataset.filter(largeDataset.col("id").mod(2).equalTo(0)).count();
        System.out.println("Even IDs count: " + count1);

        long count2 = largeDataset.filter(largeDataset.col("id").mod(3).equalTo(0)).count();
        System.out.println("Divisible by 3 IDs count: " + count2);

        // Don't forget to unpersist if the dataset is no longer needed to free up memory.
        largeDataset.unpersist();

        // 2. Reduce Data Skew: Skewed data can lead to OOM errors and slow tasks.
        // While not a code example here, techniques include salting keys or using AQE (Adaptive Query Execution).

        // 3. Proper Data Structures: Use more memory-efficient data structures if possible.
        // Tungsten helps with this internally for Spark SQL.

        // 4. Tune Garbage Collection: For long-running applications, consider JVM GC tunings.
        // Often set via spark-submit: --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16m"
        // G1GC is generally a good choice for multi-gigabyte heaps.

        spark.stop();
    }
}