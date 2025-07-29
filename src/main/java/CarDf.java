package mlib;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

public class CarDf {

    public static List<String> getNonnumeric(StructField[] fields){
        List<String> res =  new ArrayList<>();
        for(StructField field : fields){
            DataType dtype =field.dataType();
            if(dtype.sameType(DataTypes.StringType))
                res.add(field.name());
            else
            if(!(dtype.sameType(DataTypes.IntegerType) &&
                    dtype.sameType(DataTypes.DoubleType) && dtype.sameType(DataTypes.FloatType)
            &&dtype.sameType(DataTypes.LongType) && dtype.sameType(DataTypes.ShortType)
            && dtype.sameType(DataTypes.ByteType))){
                res.add(field.name());

            }
        }
        return res;


    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Iris")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> carDf = spark.read().option("header", "true")
                .option("inferSchema", "true")
                .csv("data/car-prices.csv");
        carDf =carDf.drop("car_ID");
        List<String> nonNumeric =getNonnumeric(carDf.schema().fields());
        System.out.println(nonNumeric);
        List<String> inputs =  new ArrayList<>();
        String columns="symboling,CarName,fueltype,aspiration,doornumber," +
                "carbody,drivewheel,enginelocation,wheelbase,carlength,carwidth,carheight" +
                ",curbweight,enginetype,cylindernumber,enginesize,fuelsystem,boreratio,stroke," +
                "compressionratio,horsepower,peakrpm,citympg,highwaympg";
        for(String column: columns.split(",")){
            if(nonNumeric.contains(column)){
                inputs.add(column+"1");
            }else{
                inputs.add(column);
            }
        }

        StringIndexer index = new StringIndexer().setInputCols(nonNumeric.toArray(String[]::new))
                    .setOutputCols(nonNumeric.stream().map(colum->colum+"1").toArray(String[]::new));
        //thses converts all our input cplumns to feature column as an array
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(inputs.toArray(String[]::new))
        .setOutputCol("features");
        double[] weights = {0.5, 0.5};
        //used to divide our data to training and testing
        Dataset<Row> data[] = carDf.randomSplit(weights, 10L);
        Dataset<Row> trainingData = data[0];
        Dataset<Row> testingData = data[1];
        DecisionTreeRegressor model = new DecisionTreeRegressor()
                .setLabelCol("price")
                .setFeaturesCol("features").setMaxBins(64);

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
              index,  vectorAssembler, model
        });

        PipelineModel actualModel = pipeline.fit(trainingData);//here actual model is made
        //Allowing prediction on test data
        Dataset<Row> predictions = actualModel.transform(testingData);
        predictions.show();
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("price")
                .setPredictionCol("prediction")
                .setMetricName("r2");
        System.out.println(evaluator.evaluate(predictions));
    }
}
