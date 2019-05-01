import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


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

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._


    val df = spark.read.format("org.apache.spark.csv").option("header", true).option("delimiter", ";").csv("/airbnb/out.csv")

    val amen = df.select("Amenities", "Price", "Country")

    val amenSample = amen.sample(.001)

    //val stuff = amenSample.rdd.collect

    val CP = df.select("Country","Price","Property Type", "Accommodates", "Amenities", "City")

    val noNull1 = CP.where(df.col("Price").isNotNull)

    val noNull2 = noNull1.where(df.col("Country").isNotNull)

    val noNull3 = noNull2.where(df.col("Property Type").isNotNull)

    val noNull4 = noNull3.where(df.col("Accommodates").isNotNull)

    val noNull5 = noNull4.where(df.col("Amenities").isNotNull)

    val noNull6 = noNull5.where(df.col("City").isNotNull)

    //mapreduce to get total listings per country and total price per country
    //val hmm = noNull4.rdd.map(row => ( (row.getString(0),  row.getString(2)), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ).reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))

    val eylmao = noNull5.rdd.flatMap(row =>  row.getString(4).split(",").map(am => ( (row.getString(0), am), (row.getString(1).toInt/row.getString(3).toInt, 1) ) ) )
    val lmao = eylmao.reduceByKey((x:(Int, Int), y:(Int, Int)) => (x._1+y._1, x._2+y._2))
    //make 5 columns in dataset
    //country, property type, average per night per person, number of listings, total combined price/people
    val hmmst = lmao.map(row => (row._1._1, row._1._2, row._1._3, (row._2._1/row._2._2), row._2._2, row._2._1) )

    val numPerHT = noNull5.rdd.map(row => ( row.getString(0), ( 1)  ) )

    val numPerHTred = numPerHT.reduceByKey((x:(Int), y:(Int)) => (x+y))


    // key, av price of amenity, number of listings in region that had it
    val fml = lmao.map(row => (row._1, (row._2._1/row._2._2), row._2._2))

    //val defdef = fml.map(row => ( (row._1._1, row._1._2), row._1._3, row._2, row._3))
    val defdef = fml.map(row => ( (row._1._1), row._1._2, row._2, row._3)).filter(row => row._4 > 10)

    val joined1 = defdef.toDF.join(numPerHTred.toDF, "_1")

    val huh = joined1.map(row => (row.getString(0), row.getString(1), row.getInt(2), row.getInt(3).toFloat/row.getInt(4).toFloat  ))

    val oof = huh.where(huh.col("_4").gt(.4))

    import org.apache.spark.sql.functions._

    val jj = oof.groupBy(col("_1")).agg(sort_array(collect_list(col("_2"))) as "value")

    val CountryHT_AvAmenities = jj.orderBy(desc("_1"))

    val newland = CountryHT_AvAmenities.map(row => (row.getString(0), row.getList(1).toString))

    newland.coalesce(1).write.csv("/results/CountryOnly_AvAmenities2.csv")

    val fileRdd = spark.sparkContext.parallelize(sorted.collect(), 1)

    fileRdd.saveAsTextFile("/debug3/me5thanks8")

    spark.stop();
  }
}
