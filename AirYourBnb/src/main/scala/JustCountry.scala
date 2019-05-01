import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession


object JustCountry {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("AirYourBnb-CountryOnly")
      .getOrCreate()

    val sparkContext = spark.sparkContext;

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val anthony_hdfs = "hdfs:///cs455/termproject/airbnb-listings.csv"
    val daniel_hdfs = "hdfs:///cs455/TERM/airbnb-listings.csv"
    val bruce_hdfs = "hdfs:///airbnb/out.csv"

    val user = args(0).toInt
    val filename =
      user match {
        case 1  => anthony_hdfs
        case 2  => bruce_hdfs
        case 3  => daniel_hdfs
      }


    val df = spark.read.format("org.apache.spark.csv").option("header", true).option("delimiter", ";").csv(filename)

    val table = df.select("Country","Price","Property Type", "Accommodates", "Amenities", "City")

    //This scrapes the table and removes all of the invalid entries that contain null as a value
    val notNull = table.where(df.col("Country").isNotNull).where(df.col("Price").isNotNull)
      .where(df.col("Property Type").isNotNull).where(df.col("Accommodates").isNotNull)
      .where(df.col("Amenities").isNotNull)

    //mapreduce to get total listings per country and total price per country
    //val hmm = noNull4.rdd.map(row => ( (row.getString(0),  row.getString(2)), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ).reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))

    val amenitiesGroup_ListingPrice_NumListings = notNull.rdd.flatMap(row =>  row.getString(4).split(",").map(am => ( (row.getString(0), am), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ) )
      .reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))
    //make 5 columns in dataset
    //country, property type, average per night per person, number of listings, total combined price/people


    val total_listing_types = notNull.rdd.map(row => ( row.getString(0), ( 1)  ) )
      .reduceByKey((x:(Int), y:(Int)) => (x+y))

    // key, av price of amenity, number of listings in region that had it
    val aln_rmv_ppl = amenitiesGroup_ListingPrice_NumListings.map(row => (row._1, (row._2._1/row._2._2), row._2._2))

    //val defdef = fml.map(row => ( (row._1._1, row._1._2), row._1._3, row._2, row._3))
    val filterNumListings = aln_rmv_ppl.map(row => ( (row._1._1), row._1._2, row._2, row._3)).filter(row => row._4 > 10)

    val joinedData = filterNumListings.toDF.join(total_listing_types.toDF, "_1")

    val amenity_percentage = joinedData.map(row => (row.getString(0), row.getString(1), row.getInt(2), row.getInt(3).toFloat/row.getInt(4).toFloat  ))

    val amenity_percentage_filtered = amenity_percentage.where(amenity_percentage.col("_4").gt(.4))

    import org.apache.spark.sql.functions._

    val avg_amenities_CHT = amenity_percentage_filtered.groupBy(col("_1")).agg(sort_array(collect_list(col("_2"))) as "value")

    val CountryHT_AvAmenities = avg_amenities_CHT.orderBy(desc("_1"))

    val newland = CountryHT_AvAmenities.map(row => (row.getString(0), row.getList(1).toString))

    newland.coalesce(1).write.csv("/results/CountryOnly_Avg_Amenities.csv")

    spark.stop();
  }
}
