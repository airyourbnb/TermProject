
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


        //writeAFile("TEST MESSAGE", "hdfs:///debug2/test41", sparkContext);


        //SQLContext sqlContext = new SQLContext(sparkContext);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> df = sparkSession.read().format("org.apache.spark.csv").option("header",true).option("inferSchema", true).option("delimiter",";").csv("hdfs:///airbnb/airbnb-listings.csv");




        //writeAFile("Made it here", "hdfs:///debug2/check11", sparkContext);

        String[] cols = df.columns();

        //writeAFile("Made it here", "hdfs:///debug2/check29", sparkContext);


        String result = "";
        for (String s : cols) {
            result += " | " + s;
        }


        //writeAFile("Made it here", "hdfs:///debug2/check31", sparkContext);


        writeAFile(result, "hdfs:///debug/column_name1570", sparkContext);
        //writeAFile("why oh whyyy", "hdfs:///debug/column_name1568", sparkContext);


        /*
        df.select("year", "model").write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("newcars.csv");



        JavaRDD<String> inputFile = sparkContext.read().format("hdfs://jackson:2084/airbnb/airbnb-listings-schema.json").option("header","true").load("hdfs://jackson:2084/airbnb/airbnb-listings.csv");

        JavaRDD<String> wordsFromFile = inputFile.take(30);
        */
    }

    public static void writeAFile(String message, String filePath, JavaSparkContext SpContext) {
        ArrayList<String> temp = new ArrayList<String>();

        temp.add(message);

        JavaRDD<String> test = SpContext.parallelize(temp, 1);

        test.saveAsTextFile(filePath);
    }


    public static void main(String[] args) {

        //if (args.length == 0) {
        //    System.out.println("No files provided.");
        //    System.exit(0);
        //}

        //SparkConf sparkConf = new SparkConf();

        //JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //writeAFile("baby got baka", "/debug_logs/babyGotBack", sparkContext);

        SparkConf sparkConf = new SparkConf();//.setMaster("").setAppName("JD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //writeAFile("TEST MESSAGE", "hdfs:///debug_logs2/babyGotBackaa", sparkContext);

        System.out.println("I AM INSIDE THE PROGRAM");

        //TODO make sure to modify this when we run our code
        //wordCount("hdfs://dover:42080/cs455/termproject/airbnb-listing.csv");
        wordCount("hdfs://jackson:2084/airbnb/airbnb-listings.csv", sparkContext);
    }
}
