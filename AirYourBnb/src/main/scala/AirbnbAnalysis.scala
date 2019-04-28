import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession

/*

https://stackoverflow.com/questions/40800920/how-do-i-convert-arrayrow-to-dataframe
 */

object AirbnbAnalysis {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("AirYourBnb")
      .getOrCreate()

    val sparkContext = spark.sparkContext;

    val anthony_hdfs = "hdfs:///cs455/termproject/airbnb-listings.csv"
    val daniel_hdfs = "hdfs:///cs455/TERM/airbnb-listings.csv"
    val bruce_hdfs = "hdfs:///airbnb/out.csv"

    val user = args(0)
    val filename = ""

    user match {
      case 1  => filename = anthony_hdfs
      case 2  => filename = bruce_hdfs
      case 3  => filename = daniel_hdfs
    }

    val df = spark.read.format("org.apache.spark.csv").option("header", true).option("delimiter", ";").csv(filename)

    val table = df.select("ID", "Country", "Property Type", "Amenities", "Price", "Accommodates")
    

/*
    val amenSample = amen.sample(.001)

    val stuff = amenSample.rdd.collect

    val fileRdd = spark.sparkContext.parallelize(stuff, 1)
*/
    fileRdd.saveAsTextFile("/debug3/me4thanks6")
    //val fileRDD = spark.sparkContext.parallelize(amens.sample(.01), 1);
    //fileRDD.saveAsTextFile("/debug3/whaat2");



    spark.stop();
  }
}
