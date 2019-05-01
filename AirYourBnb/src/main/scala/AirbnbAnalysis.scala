import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession

/*

https://stackoverflow.com/questions/40800920/how-do-i-convert-arrayrow-to-dataframe
 */

object AirbnbAnalysis {
  def main(args: Array[String]) {

    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val spark = SparkSession
      .builder
      .appName("AirYourBnb")
      .getOrCreate()

    val sparkContext = spark.sparkContext;
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

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

    val table = df.select("Country","Price","Property Type", "Accommodates", "Amenities", "City")

    //This scrapes the table and removes all of the invalid entries that contain null as a value
    val notNull = table.where(df.col("Country").isNotNull).where(df.col("Price").isNotNull).where(df.col("Property Type").isNotNull).where(df.col("Accommodates").isNotNull).where(df.col("Amenities").isNotNull)

    //FIXME
    val lmao = notNull.rdd.flatMap(row =>  row.getString(4).split(",").map(am => ( (row.getString(0),  row.getString(2), am), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ) ).reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))
    val hmmst = lmao.map(row => (row._1._1, row._1._2, row._1._3, (row._2._1/row._2._2), row._2._2, row._2._1) )

    val total_listing_types = notNull.rdd.map(row => ( (row.getString(0),  row.getString(2)), ( 1) ) )
        .reduceByKey((x:(Int), y:(Int)) => (x+y))

    spark.stop();
  }
}
