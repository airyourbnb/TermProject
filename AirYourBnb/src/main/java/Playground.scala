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

    val fileRDD = spark.sparkContext.parallelize(df.columns, 1);

    fileRDD.saveAsTextFile("/debug3/whaatttt");

    spark.stop();
  }
}

