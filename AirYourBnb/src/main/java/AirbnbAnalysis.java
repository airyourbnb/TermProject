import java.io.Serializable;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.Arrays;

public class AirbnbAnalysis{



    //When we submit the job pass our hdfs cluster as a single arg 1 2 or 3
    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        SparkConf sparkConf = new SparkConf();//.setMaster("").setAppName("JD Word Counter");
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        String fileName = null;
        String anthony_hdfs = "hdfs://dover.cs.colostate.edu:42080/cs455/termproject/airbnb-listings.csv";
        String daniel_hdfs = "hdfs://juneau.cs.colostate.edu:11223/cs455/TERM/airbnb-listings.csv";
        String bruce_hdfs = "hdfs://jackson:2084/airbnb/airbnb-listings.csv";

        if(args.length != 1){
            System.err.println("Supply your HDFS cluster information");
            System.exit(-1);
        }

        switch(Integer.parseInt(args[0])){
            case 1:
                fileName = anthony_hdfs;
                break;
            case 2:
                fileName = daniel_hdfs;
                break;
            case 3:
                fileName = bruce_hdfs;
                break;
        }

        Dataset<Row> csv = spark.read().format("csv").option("header","true").option("delimiter", ";").load(fileName);
        //csv.show();
        csv.write().json("/cs455/termproject/temp-data.json");
//        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
//
//        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
//
//        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

    }

}
