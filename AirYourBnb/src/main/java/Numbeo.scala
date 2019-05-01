import java.io.File

import scala.io.Source._

import org.apache.spark.sql._

import org.apache.spark.sql.functions.explode


object Numbeo {
  def main(args: Array[String]) {

    /*
    val spark = SparkSession
      .builder
      .appName("DFS Read Write Test")
      .getOrCreate()

    val sparkContext = spark.sparkContext

    val df = spark.read.format("org.apache.spark.json").option("multiline", "true").json("hdfs:///cs455/TERM/numbeo-data-formated.json")

    val explodedCountry = df.select(explode(df("Index_Set_Country"))).toDF("Country")
    val explodedCity = df.select(explode(df("Index_Set_City"))).toDF("City")

    val dfSplitCountry = explodedCountry.select("Country.name","Country.rent_index")
    val dfSplitCity = explodedCity.select("City.name","City.rent_index")

    val dfSplitNoNullCountry = dfSplitCountry.where(dfSplitCountry.col("rent_index").isNotNull)
    val dfSplitNoNullCity = dfSplitCity.where(dfSplitCity.col("rent_index").isNotNull)

    val sortedCountry = dfSplitNoNullCountry.sort($"rent_index".desc)
    val sortedCity = dfSplitNoNullCity.sort($"rent_index".desc)

    val fileRddCountry = sortedCountry.rdd
    val fileRddCity = sortedCity.rdd

    fileRddCountry.saveAsTextFile("/debug/testCountry")
    fileRddCity.saveAsTextFile("/debug/testCity")

    spark.stop()
    */
  }
}