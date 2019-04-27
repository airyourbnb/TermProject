import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession

object Playground {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("DFS Read Write Test")
      .getOrCreate()

    val sparkContext = spark.sparkContext;

    val df = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).option("delimiter", ";").csv("hdfs:///airbnb/airbnb-listings.csv");

    val amen = df.select("Amenities")

    val amenSample = amen.sample(.001)

    val stuff = amenSample.rdd.collect

    val fileRdd = spark.sparkContext.parallelize(stuff, 1)

    fileRdd.saveAsTextFile("/debug3/metoothanks2me2thanks2")
    //val fileRDD = spark.sparkContext.parallelize(amens.sample(.01), 1);
    //fileRDD.saveAsTextFile("/debug3/whaat2");



    spark.stop();
  }
}

