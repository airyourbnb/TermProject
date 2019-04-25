
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount {

    private static void wordCount(String fileName) {

        SparkConf sparkConf = new SparkConf();//.setMaster("").setAppName("JD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        //With this example where does this actually save the data?
        countData.saveAsTextFile("CountData");
    }

    public static void main(String[] args) {

        //if (args.length == 0) {
        //    System.out.println("No files provided.");
        //    System.exit(0);
        //}

        //TODO make sure to modify this when we run our code
        wordCount("hdfs://dover:42080/cs455/termproject/airbnb-listing.csv");
        //wordCount("hdfs://jackson:2084/airbnb/airbnb-listings.csv");
    }
}
