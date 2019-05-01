import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Numbeo {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Numbeo Data Split").getOrCreate()

    import spark.implicits._

    val sparkContext = spark.sparkContext

    //TODO we all need to upload this into our clusters
    val df = spark.read.format("org.apache.spark.json").option("multiline", "true").json("hdfs:///cs455/TERM/numbeo-data-formated.json")

    val explodedCountry = df.select(explode(df("Index_Set_Country"))).toDF("Country")
    val explodedCity = df.select(explode(df("Index_Set_City"))).toDF("City")

    val dfSplitCountry = explodedCountry.select("Country.name","Country.rent_index")
    val dfSplitCity = explodedCity.select("City.name","City.rent_index")

    val dfSplitCountryClI = explodedCountry.select("Country.name","Country.climate_index")
    val dfSplitCountryCPIRI = explodedCountry.select("Country.name","Country.cpi_and_rent_index")
    val dfSplitCountryCPII = explodedCountry.select("Country.name","Country.cpi_index")
    val dfSplitCountryCrI = explodedCountry.select("Country.name","Country.crime_index")
    val dfSplitCountryGI = explodedCountry.select("Country.name","Country.groceries_index")
    val dfSplitCountryHCI = explodedCountry.select("Country.name","Country.health_care_index")
    val dfSplitCountryPI = explodedCountry.select("Country.name","Country.pollution_index")
    val dfSplitCountryPPTIR = explodedCountry.select("Country.name","Country.property_price_to_income_ratio")
    val dfSplitCountryPPIRI = explodedCountry.select("Country.name","Country.purchasing_power_incl_rent_index")
    val dfSplitCountryQOLI = explodedCountry.select("Country.name","Country.quality_of_life_index")
    val dfSplitCountryRPI = explodedCountry.select("Country.name","Country.restaurant_price_index")
    val dfSplitCountrySI = explodedCountry.select("Country.name","Country.safety_index")
    val dfSplitCountryTCI = explodedCountry.select("Country.name","Country.traffic_co2_index")
    val dfSplitCountryTI = explodedCountry.select("Country.name","Country.traffic_index")
    val dfSplitCountryTII = explodedCountry.select("Country.name","Country.traffic_inefficiency_index")
    val dfSplitCountryTTI = explodedCountry.select("Country.name","Country.traffic_time_index")


    val dfSplitNoNullCountry = dfSplitCountry.where(dfSplitCountry.col("rent_index").isNotNull)
    val dfSplitNoNullCity = dfSplitCity.where(dfSplitCity.col("rent_index").isNotNull)

    val dfSplitNoNullCountryClI = dfSplitCountryClI.where(dfSplitCountryClI.col("climate_index").isNotNull)
    val dfSplitNoNullCountryCPIRI = dfSplitCountryCPIRI.where(dfSplitCountryCPIRI.col("cpi_and_rent_index").isNotNull)
    val dfSplitNoNullCountryCPII = dfSplitCountryCPII.where(dfSplitCountryCPII.col("cpi_index").isNotNull)
    val dfSplitNoNullCountryCrI = dfSplitCountryCrI.where(dfSplitCountryCrI.col("crime_index").isNotNull)
    val dfSplitNoNullCountryGI = dfSplitCountryGI.where(dfSplitCountryGI.col("groceries_index").isNotNull)
    val dfSplitNoNullCountryHCI = dfSplitCountryHCI.where(dfSplitCountryHCI.col("health_care_index").isNotNull)
    val dfSplitNoNullCountryPI = dfSplitCountryPI.where(dfSplitCountryPI.col("pollution_index").isNotNull)
    val dfSplitNoNullCountryPPTIR = dfSplitCountryPPTIR.where(dfSplitCountryPPTIR.col("property_price_to_income_ratio").isNotNull)
    val dfSplitNoNullCountryPPIRI = dfSplitCountryPPIRI.where(dfSplitCountryPPIRI.col("purchasing_power_incl_rent_index").isNotNull)
    val dfSplitNoNullCountryQOLI = dfSplitCountryQOLI.where(dfSplitCountryQOLI.col("quality_of_life_index").isNotNull)
    val dfSplitNoNullCountryRPI = dfSplitCountryRPI.where(dfSplitCountryRPI.col("restaurant_price_index").isNotNull)
    val dfSplitNoNullCountrySI = dfSplitCountrySI.where(dfSplitCountrySI.col("safety_index").isNotNull)
    val dfSplitNoNullCountryTCI = dfSplitCountryTCI.where(dfSplitCountryTCI.col("traffic_co2_index").isNotNull)
    val dfSplitNoNullCountryTI = dfSplitCountryTI.where(dfSplitCountryTI.col("traffic_index").isNotNull)
    val dfSplitNoNullCountryTII = dfSplitCountryTII.where(dfSplitCountryTII.col("traffic_inefficiency_index").isNotNull)
    val dfSplitNoNullCountryTTI = dfSplitCountryTTI.where(dfSplitCountryTTI.col("traffic_time_index").isNotNull)


    val sortedCountry = dfSplitNoNullCountry.sort($"rent_index".desc)
    val sortedCity = dfSplitNoNullCity.sort($"rent_index".desc)

    val sortedCountryClI = dfSplitNoNullCountryClI.sort($"climate_index".desc)
    val sortedCountryCPIRI = dfSplitNoNullCountryCPIRI.sort($"cpi_and_rent_index".desc)
    val sortedCountryCPII = dfSplitNoNullCountryCPII.sort($"cpi_index".desc)
    val sortedCountryCrI = dfSplitNoNullCountryCrI.sort($"crime_index".desc)
    val sortedCountryGI = dfSplitNoNullCountryGI.sort($"groceries_index".desc)
    val sortedCountryHCI = dfSplitNoNullCountryHCI.sort($"health_care_index".desc)
    val sortedCountryPI = dfSplitNoNullCountryPI.sort($"pollution_index".desc)
    val sortedCountryPPTIR = dfSplitNoNullCountryPPTIR.sort($"property_price_to_income_ratio".desc)
    val sortedCountryPPIRI = dfSplitNoNullCountryPPIRI.sort($"purchasing_power_incl_rent_index".desc)
    val sortedCountryQOLI = dfSplitNoNullCountryQOLI.sort($"quality_of_life_index".desc)
    val sortedCountryRPI = dfSplitNoNullCountryRPI.sort($"restaurant_price_index".desc)
    val sortedCountrySI = dfSplitNoNullCountrySI.sort($"safety_index".desc)
    val sortedCountryTCI = dfSplitNoNullCountryTCI.sort($"traffic_co2_index".desc)
    val sortedCountryTI = dfSplitNoNullCountryTI.sort($"traffic_index".desc)
    val sortedCountryTII = dfSplitNoNullCountryTII.sort($"traffic_inefficiency_index".desc)
    val sortedCountryTTI = dfSplitNoNullCountryTTI.sort($"traffic_time_index".desc)

    val sortedCountryFlipped = dfSplitNoNullCountry.sort($"rent_index".asc)
    val sortedCountryClIFlipped = dfSplitNoNullCountryClI.sort($"climate_index".asc)
    val sortedCountryCPIRIFlipped = dfSplitNoNullCountryCPIRI.sort($"cpi_and_rent_index".asc)
    val sortedCountryCPIIFlipped = dfSplitNoNullCountryCPII.sort($"cpi_index".asc)
    val sortedCountryCrIFlipped = dfSplitNoNullCountryCrI.sort($"crime_index".asc)
    val sortedCountryGIFlipped = dfSplitNoNullCountryGI.sort($"groceries_index".asc)
    val sortedCountryHCIFlipped = dfSplitNoNullCountryHCI.sort($"health_care_index".asc)
    val sortedCountryPIFlipped = dfSplitNoNullCountryPI.sort($"pollution_index".asc)
    val sortedCountryPPTIRFlipped = dfSplitNoNullCountryPPTIR.sort($"property_price_to_income_ratio".asc)
    val sortedCountryPPIRIFlipped = dfSplitNoNullCountryPPIRI.sort($"purchasing_power_incl_rent_index".asc)
    val sortedCountryQOLIFlipped = dfSplitNoNullCountryQOLI.sort($"quality_of_life_index".asc)
    val sortedCountryRPIFlipped = dfSplitNoNullCountryRPI.sort($"restaurant_price_index".asc)
    val sortedCountrySIFlipped = dfSplitNoNullCountrySI.sort($"safety_index".asc)
    val sortedCountryTCIFlipped = dfSplitNoNullCountryTCI.sort($"traffic_co2_index".asc)
    val sortedCountryTIFlipped = dfSplitNoNullCountryTI.sort($"traffic_index".asc)
    val sortedCountryTIIFlipped = dfSplitNoNullCountryTII.sort($"traffic_inefficiency_index".asc)
    val sortedCountryTTIFlipped = dfSplitNoNullCountryTTI.sort($"traffic_time_index".asc)


    val fileRddCountry = spark.sparkContext.parallelize(sortedCountry.rdd.collect(), 1)
    val fileRddCity = spark.sparkContext.parallelize(sortedCity.rdd.collect(), 1)

    val fileRddCountryClI = spark.sparkContext.parallelize(sortedCountryClI.rdd.collect(), 1)
    val fileRddCountryCPIRI = spark.sparkContext.parallelize(sortedCountryCPIRI.rdd.collect(), 1)
    val fileRddCountryCPII = spark.sparkContext.parallelize(sortedCountryCPII.rdd.collect(), 1)
    val fileRddCountryCrI = spark.sparkContext.parallelize(sortedCountryCrI.rdd.collect(), 1)
    val fileRddCountryGI = spark.sparkContext.parallelize(sortedCountryGI.rdd.collect(), 1)
    val fileRddCountryHCI = spark.sparkContext.parallelize(sortedCountryHCI.rdd.collect(), 1)
    val fileRddCountryPI = spark.sparkContext.parallelize(sortedCountryPI.rdd.collect(), 1)
    val fileRddCountryPPTIR = spark.sparkContext.parallelize(sortedCountryPPTIR.rdd.collect(), 1)
    val fileRddCountryPPIRI = spark.sparkContext.parallelize(sortedCountryPPIRI.rdd.collect(), 1)
    val fileRddCountryQOLI = spark.sparkContext.parallelize(sortedCountryQOLI.rdd.collect(), 1)
    val fileRddCountryRPI = spark.sparkContext.parallelize(sortedCountryRPI.rdd.collect(), 1)
    val fileRddCountrySI = spark.sparkContext.parallelize(sortedCountrySI.rdd.collect(), 1)
    val fileRddCountryTCI = spark.sparkContext.parallelize(sortedCountryTCI.rdd.collect(), 1)
    val fileRddCountryTI = spark.sparkContext.parallelize(sortedCountryTI.rdd.collect(), 1)
    val fileRddCountryTII = spark.sparkContext.parallelize(sortedCountryTII.rdd.collect(), 1)
    val fileRddCountryTTI = spark.sparkContext.parallelize(sortedCountryTTI.rdd.collect(), 1)

    val fileRddCountryFlipped = spark.sparkContext.parallelize(sortedCountryFlipped.rdd.collect(), 1)
    val fileRddCountryClIFlipped = spark.sparkContext.parallelize(sortedCountryClIFlipped.rdd.collect(), 1)
    val fileRddCountryCPIRIFlipped = spark.sparkContext.parallelize(sortedCountryCPIRIFlipped.rdd.collect(), 1)
    val fileRddCountryCPIIFlipped = spark.sparkContext.parallelize(sortedCountryCPIIFlipped.rdd.collect(), 1)
    val fileRddCountryCrIFlipped = spark.sparkContext.parallelize(sortedCountryCrIFlipped.rdd.collect(), 1)
    val fileRddCountryGIFlipped = spark.sparkContext.parallelize(sortedCountryGIFlipped.rdd.collect(), 1)
    val fileRddCountryHCIFlipped = spark.sparkContext.parallelize(sortedCountryHCIFlipped.rdd.collect(), 1)
    val fileRddCountryPIFlipped = spark.sparkContext.parallelize(sortedCountryPIFlipped.rdd.collect(), 1)
    val fileRddCountryPPTIRFlipped = spark.sparkContext.parallelize(sortedCountryPPTIRFlipped.rdd.collect(), 1)
    val fileRddCountryPPIRIFlipped = spark.sparkContext.parallelize(sortedCountryPPIRIFlipped.rdd.collect(), 1)
    val fileRddCountryQOLIFlipped = spark.sparkContext.parallelize(sortedCountryQOLIFlipped.rdd.collect(), 1)
    val fileRddCountryRPIFlipped = spark.sparkContext.parallelize(sortedCountryRPIFlipped.rdd.collect(), 1)
    val fileRddCountrySIFlipped = spark.sparkContext.parallelize(sortedCountrySIFlipped.rdd.collect(), 1)
    val fileRddCountryTCIFlipped = spark.sparkContext.parallelize(sortedCountryTCIFlipped.rdd.collect(), 1)
    val fileRddCountryTIFlipped = spark.sparkContext.parallelize(sortedCountryTIFlipped.rdd.collect(), 1)
    val fileRddCountryTIIFlipped = spark.sparkContext.parallelize(sortedCountryTIIFlipped.rdd.collect(), 1)
    val fileRddCountryTTIFlipped = spark.sparkContext.parallelize(sortedCountryTTIFlipped.rdd.collect(), 1)

    fileRddCountry.saveAsTextFile("/debug/testCountry")
    fileRddCity.saveAsTextFile("/debug/testCity")

    fileRddCountryClI.saveAsTextFile("/results/climate_index")
    fileRddCountryCPIRI.saveAsTextFile("/results/cpi_and_rent_index")
    fileRddCountryCPII.saveAsTextFile("/results/cpi_index")
    fileRddCountryCrI.saveAsTextFile("/results/crime_index")
    fileRddCountryGI.saveAsTextFile("/results/groceries_index")
    fileRddCountryHCI.saveAsTextFile("/results/health_care_index")
    fileRddCountryPI.saveAsTextFile("/results/pollution_index")
    fileRddCountryPPTIR.saveAsTextFile("/results/property_price_to_income_ratio")
    fileRddCountryPPIRI.saveAsTextFile("/results/purchasing_power_incl_rent_index")
    fileRddCountryQOLI.saveAsTextFile("/results/quality_of_life_index")
    fileRddCountryRPI.saveAsTextFile("/results/restaurant_price_index")
    fileRddCountrySI.saveAsTextFile("/results/safety_index")
    fileRddCountryTCI.saveAsTextFile("/results/traffic_co2_index")
    fileRddCountryTI.saveAsTextFile("/results/traffic_index")
    fileRddCountryTII.saveAsTextFile("/results/traffic_inefficiency_index")
    fileRddCountryTTI.saveAsTextFile("/results/traffic_time_index")

    fileRddCountryFlipped.saveAsTextFile("/results/flipped/rent_index")
    fileRddCountryClIFlipped.saveAsTextFile("/results/flipped/climate_index")
    fileRddCountryCPIRIFlipped.saveAsTextFile("/results/flipped/cpi_and_rent_index")
    fileRddCountryCPIIFlipped.saveAsTextFile("/results/flipped/cpi_index")
    fileRddCountryCrIFlipped.saveAsTextFile("/results/flipped/crime_index")
    fileRddCountryGIFlipped.saveAsTextFile("/results/flipped/groceries_index")
    fileRddCountryHCIFlipped.saveAsTextFile("/results/flipped/health_care_index")
    fileRddCountryPIFlipped.saveAsTextFile("/results/flipped/pollution_index")
    fileRddCountryPPTIRFlipped.saveAsTextFile("/results/flipped/property_price_to_income_ratio")
    fileRddCountryPPIRIFlipped.saveAsTextFile("/results/flipped/purchasing_power_incl_rent_index")
    fileRddCountryQOLIFlipped.saveAsTextFile("/results/flipped/quality_of_life_index")
    fileRddCountryRPIFlipped.saveAsTextFile("/results/flipped/restaurant_price_index")
    fileRddCountrySIFlipped.saveAsTextFile("/results/flipped/safety_index")
    fileRddCountryTCIFlipped.saveAsTextFile("/results/flipped/traffic_co2_index")
    fileRddCountryTIFlipped.saveAsTextFile("/results/flipped/traffic_index")
    fileRddCountryTIIFlipped.saveAsTextFile("/results/flipped/traffic_inefficiency_index")
    fileRddCountryTTIFlipped.saveAsTextFile("/results/flipped/traffic_time_index")

    spark.stop()
  }
}
