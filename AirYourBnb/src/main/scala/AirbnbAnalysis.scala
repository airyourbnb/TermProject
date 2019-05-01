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
    .appName("AirYourBnb-CountryHT")
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

    //amenitiesGroup_ListingPrice_NumListings => tuple version that contains (Country, HT, Amenity), (Int total listing price, # of listings)
    val amenitiesGroup_ListingPrice_NumListings = notNull.rdd.flatMap(row =>  row.getString(4).split(",")
      .map(am => ( (row.getString(0),  row.getString(2), am), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ) )
      .reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))

    //aln_flattend => a flattened version ofamenitiesGroup_ListingPrice_NumListings that contains the (Country, HT, Amenitiy, pricePerNight4Amenity, #ofListings, total_listing price )
    val aln_flattend =amenitiesGroup_ListingPrice_NumListings.map(row => (row._1._1, row._1._2, row._1._3, (row._2._1/row._2._2), row._2._2, row._2._1) )

    //Combination of numPerHT and numPerHTred
    val total_listing_types = notNull.rdd.map(row => ( (row.getString(0),  row.getString(2)), ( 1) ) )
      .reduceByKey((x:(Int), y:(Int)) => (x+y))

    //aln_rmv_ppl => amenitiesGroup_ListingPrice_NumListings removed Price per listing
    //(Country, HT, Amenity), pricePerNight4Amenity, #ofListings => takes out the price per listing
    val aln_rmv_ppl = amenitiesGroup_ListingPrice_NumListings.map(row => (row._1, (row._2._1/row._2._2), row._2._2))

    //filterNumListings
    //(Country,HT),Amenity, pricePerNight4Amenity, #ofListings and filters out the entries where #of listings is greater than 63
    val filterNumListings = aln_rmv_ppl.map(row => ( (row._1._1, row._1._2), row._1._3, row._2, row._3)).filter(row => row._4 > 250)

    //Joins filterNumListings with total_listing_types by using (Country, HT as a foreign key)
    val joinedData = filterNumListings.toDF.join(total_listing_types.toDF, "_1")

    //This calculates the amenity percentage of there presence within that listing type
    //(Country,HT),Amenity ,PricePerNight,(#ofListingsWithAmenity / Total#oflistings)
    val amenity_percentage = joinedData.map(row => (row.getStruct(0).toString, row.getString(1), row.getInt(2), row.getInt(3).toFloat/row.getInt(4).toFloat  ))

    //This filters out the data where the amenity is present in at least 40% of the listings
    val amenity_percentage_filtered = amenity_percentage.where(amenity_percentage.col("_4").gt(.4))

    import org.apache.spark.sql.functions._

    //[Country, HT], [List of the amenities]
    val avg_amenities_CHT = amenity_percentage_filtered.groupBy(col("_1")).agg(sort_array(collect_list(col("_2"))) as "value")

    //Ordered Country HT
    val CountryHT_AvAmenities = avg_amenities_CHT.orderBy(desc("_1"))

    //Reformatted  above to fit into 2 entries for csv
    val newland = CountryHT_AvAmenities.map(row => (row.getString(0), row.getList(1).toString))

    //Writes out to csv in your hdfs
    newland.coalesce(1).write.csv("/results/CountryHT_Avg_Amenities.csv")

    val countryRankingsMap = notNull.rdd.flatMap(row =>  row.getString(4).split(",").map(am => ( (row.getString(0)), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ) )
    val countryRankingRed = countryRankingsMap.reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))
    val countryRankingSplit = countryRankingRed.map(row => (row._1, (row._2._1/row._2._2) ) )
    val countryRanking = countryRankingSplit.toDF.orderBy(desc("_2"))

    val fileRdd = spark.sparkContext.parallelize(countryRanking.rdd.collect(), 1)
    fileRdd.saveAsTextFile("/results/CountryRankings_AirBnb_Data")


    spark.stop();
  }
}
