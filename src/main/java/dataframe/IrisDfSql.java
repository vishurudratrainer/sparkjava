package dataframe;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IrisDfSql {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Iris")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> irisDf = spark.read().option("header", "true")
                .option("inferSchema", "true")
                .csv("data/Iris.csv");

        irisDf.createTempView("iris_table");
        System.out.println("Select using SQL");
        Dataset<Row> selectDf = spark.sql("select PetalWidthCm,Species from iris_table");
        selectDf.show();
        System.out.println("Filter");
        Dataset<Row> filteDf = spark.sql("select * from iris_table where PetalWidthCm > 3.0");
        filteDf.show();
        System.out.println("Group");
        Dataset<Row> groupBy =spark.sql
                ("select Species,sum(PetalWidthCm),avg(PetalWidthCm) from iris_table " +
                        "group by Species ");
        groupBy.show();
       /** System.out.println("insert");
        spark.sql("insert into iris_table values(151,1.1,2.1,3.1,4.1,'new species')");
        System.out.println("Select after inserting");

        Dataset<Row> selectDf2 = spark.sql("select * from iris_table where id > 150");
        selectDf2.show();**/
        /**System.out.println("Update");
        spark.sql("Update iris_table set PetalWidthCm=PetalWidthCm+1 where Species ='Iris-Setosa' ").show();
  **/  }
}
