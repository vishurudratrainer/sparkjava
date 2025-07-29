package dataframe;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;

public class AuthorsDf {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Iris")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> irisDf = spark.read().option("header", "true")
                .option("inferSchema", "true")
                .csv("data/authors.csv");
        irisDf=irisDf.withColumn("external",struct(col("link"),col("wikipedia")));
        irisDf.show(5);
        irisDf=irisDf.select(col("id"),col("name"),col("external"));
        irisDf.show(5);

        Dataset<Author> authorDataset = irisDf.as(Encoders.bean(Author.class));
        authorDataset.foreach((ForeachFunction<Author>) author -> System.out.println(author));

    }
}
