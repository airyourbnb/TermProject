import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession

/*

https://stackoverflow.com/questions/40800920/how-do-i-convert-arrayrow-to-dataframe
 */

object Playground {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("DFS Read Write Test")
      .getOrCreate()

    val sparkContext = spark.sparkContext;

    val df = spark.read.format("org.apache.spark.csv").option("header", true).option("delimiter", ";").csv("hdfs:///airbnb/out.csv")

    val amen = df.select("Amenities", "Price", "Country")
    val table = df.select("ID", "Country", "Property Type", "Amenities", "Price", "Accommodates")

    val amenSample = amen.sample(.001)

    val stuff = amenSample.rdd.collect

    val fileRdd = spark.sparkContext.parallelize(stuff, 1)

    fileRdd.saveAsTextFile("/debug3/me4thanks6")
    //val fileRDD = spark.sparkContext.parallelize(amens.sample(.01), 1);
    //fileRDD.saveAsTextFile("/debug3/whaat2");



    spark.stop();
  }
}

