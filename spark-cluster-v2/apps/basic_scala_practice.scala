import org.apache.spark.sql.SparkSession
object SparkSessionTest extends App {
    val spark = SparkSession.builder().appName("My App").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val raw_content = sc.textFile("/opt/spark-data/2023-07-23-r.csv")
    raw_content.take(10).foreach(println)
}
