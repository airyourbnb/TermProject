import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class AirbnbAnalysis{



    //When we submit the job pass our hdfs cluster as a single arg 1 2 or 3
    public static void main(String[] args){

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


        SparkConf sparkConf = new SparkConf();//.setMaster("").setAppName("JD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

    }

}