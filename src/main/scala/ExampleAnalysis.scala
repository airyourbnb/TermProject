object ExampleAnalysis {

	def main(args: Array[String]) {
	val spark = SparkSession.builder.appName("ExampleAnalysis").getOrCreate()
	val sc = SParkContext.getOrCreate()
	import spark.implicits._


	}

}
