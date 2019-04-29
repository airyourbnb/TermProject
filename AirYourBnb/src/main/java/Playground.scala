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

    val df = spark.read.format("org.apache.spark.csv").option("header", true).option("delimiter", ";").csv("/airbnb/out.csv")

    val amen = df.select("Amenities", "Price", "Country")

    val amenSample = amen.sample(.001)

    val stuff = amenSample.rdd.collect

    val CP = df.select("Country","Price","Property Type", "Accommodates")

    val noNull1 = CP.where(df.col("Price").isNotNull)

    val noNull2 = noNull1.where(df.col("Country").isNotNull)

    val noNull3 = noNull2.where(df.col("Property Type").isNotNull)

    val noNull4 = noNull3.where(df.col("Accomodates").isNotNull)

    //mapreduce to get total listings per country and total price per country
    val hmm = noNull4.rdd.map(row => ( (row.getString(0),  row.getString(2)), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ).reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))

    val hmmst = hmm.map(row => (row._1._1, row._1._2, (row._2._1/row._2._2), row._2._2, row._2._1) )

    //val fileRDD = spark.sparkContext.parallelize(amens.sample(.01), 1);
    //fileRDD.saveAsTextFile("/debug3/whaat2");

    val fileRdd = spark.sparkContext.parallelize(hmmst.collect(), 1)

    fileRdd.saveAsTextFile("/debug3/me4thanks69420")

    spark.stop();
  }
}

