
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.sql.SQLContext;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;
import org.apache.spark.sql.SparkSession;

/*
https://spark.apache.org/docs/1.0.1/sql-programming-guide.html



FORMAT THAT WORKS

https://stackoverflow.com/questions/36007686/how-to-parse-a-csv-that-uses-a-i-e-001-as-the-delimiter-with-spark-csv


 */


public class TestPlay {

    private static void wordCount(String fileName, JavaSparkContext sparkContext) {

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> df = sparkSession.read().format("org.apache.spark.csv").option("header",true).option("inferSchema", true).option("delimiter",";").csv("hdfs:///airbnb/airbnb-listings.csv");

        String[] cols = df.columns();

        String result = "";
        for (String s : cols) {
            result += " | " + s;
        }

        writeAFile(result, "hdfs:///debug/column_name1570", sparkContext);




    }

    public static void writeAFile(String message, String filePath, JavaSparkContext SpContext) {
        ArrayList<String> temp = new ArrayList<String>();

        temp.add(message);

        JavaRDD<String> test = SpContext.parallelize(temp, 1);

        test.saveAsTextFile(filePath);
    }


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        System.out.println("I AM INSIDE THE PROGRAM");

        wordCount("hdfs://jackson:2084/airbnb/airbnb-listings.csv", sparkContext);
    }
}
