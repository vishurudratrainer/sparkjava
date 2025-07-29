//package mlib;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BostonDf {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Boston")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> bostonDf = spark.read().option("header", "true")
                .option("inferSchema", "true")
                .csv("data//BostonHousing.csv");
        bostonDf.printSchema();
        String[] oldColumns = bostonDf.columns();
        String[] newcolumns = Arrays.stream(oldColumns)
                .map(name -> name.toLowerCase().replace(" ", "").replace(".", "").trim())
                .toArray(String[]::new);
        List<String> inputCols = new ArrayList<>();
        for (int i = 0; i < oldColumns.length; i++) {
            bostonDf = bostonDf.withColumnRenamed(oldColumns[i], newcolumns[i]);
            if (!newcolumns[i].equals("catmedv")) inputCols.add(newcolumns[i]);
        }
        bostonDf.printSchema();


        //thses converts all our input cplumns to feature column as an array
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(inputCols.toArray(String[]::new))
                .setOutputCol("features");
        double[] weights = {0.8, 0.2};
        //used to divide our data to training and testing
        Dataset<Row> data[] = bostonDf.randomSplit(weights, 10L);
        Dataset<Row> trainingData = data[0];
        Dataset<Row> testingData = data[1];
        DecisionTreeRegressor model = new DecisionTreeRegressor()
                .setLabelCol("catmedv")
                .setFeaturesCol("features");

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                vectorAssembler, model
        });

        PipelineModel actualModel = pipeline.fit(trainingData);//here actual model is made
        //Allowing prediction on test data
        Dataset<Row> predictions = actualModel.transform(testingData);
        predictions.show();
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("catmedv")
                .setPredictionCol("prediction")
                .setMetricName("r2");
        System.out.println(evaluator.evaluate(predictions));
    }
}
