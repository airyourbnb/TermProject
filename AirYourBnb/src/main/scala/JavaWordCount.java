
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

public class JavaWordCount {

    private static void wordCount(String fileName) {

        SparkConf sparkConf = new SparkConf();//.setMaster("").setAppName("JD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);



        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("hdfs://jackson:2084/airbnb/airbnb-listings.csv");


        String[] cols = df.columns();



        String result = "";
        for(String s:cols){
            result += " " + s;
        }



        /*
        df.select("year", "model").write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("newcars.csv");



        JavaRDD<String> inputFile = sparkContext.read().format("hdfs://jackson:2084/airbnb/airbnb-listings-schema.json").option("header","true").load("hdfs://jackson:2084/airbnb/airbnb-listings.csv");

        JavaRDD<String> wordsFromFile = inputFile.take(30);

        wordsFromFile.saveAsTextFile("wordsFromFile");
        */
    }

    public static void writeAFile(String message, String filePath, JavaSparkContext SpContext){
        ArrayList<String> temp = new ArrayList<String>();

        temp.add(message);

        JavaRDD<String> test = SpContext.parallelize(temp);

        test.saveAsTextFile(filePath);
    }



    public static void main(String[] args) {

        //if (args.length == 0) {
        //    System.out.println("No files provided.");
        //    System.exit(0);
        //}

        SparkConf sparkConf = new SparkConf();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        writeAFile("baby got baka", "/debug_logs/babyGotBack", sparkContext);

        System.out.println("I AM INSIDE THE PROGRAM");

        //wordCount("hdfs://jackson:2084/airbnb/airbnb-listings.csv");
    }
}
