import org.apache.spark.sql.SparkSession
object SparkSessionTest extends App {
    val spark = SparkSession.builder().appName("My App").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    val raw_content = sc.textFile("/opt/spark-data/2023-07-23-r.csv")
    //raw_content.take(10).foreach(println)
    //raw_content.takeSample(true, 5, seed=100).foreach(println)
    var content = raw_content.map(x => x.split(','))

    def clean(x: Array[String]): Array[String] = {x.map(_.filter(_ != '"'))}
    content = content.map(clean)
    content.foreach(row => println(row.mkString(",")))
    //println(content.take(2))

    val package_count = content.map(x => (x(5), 1)).reduceByKey((a,b) => a+b)
    //println("package count class: ",package_count.getClass)
    //println(package_count.count)
    //package_count.take(5).foreach(println)
    val package_count_2 = content.map(x => (x(5), 1)).countByKey()
    //println(package_count_2("EU"))

    //package_count.map(x => (x._2, x._1)).sortByKey(true).take(5).foreach(println)
    //package_count.map(x => (x._2, x._1)).sortByKey(false).take(5).foreach(println)

    //println(content.filter(x => (x(5) == "US") || (x(5) == "AU")).count())
    //content.filter(x => (x(5) == "US") || (x(5) == "AU")).take(1)(0).foreach(println)

    val temp = content.filter(x => (x(5) == "US") || (x(5) == "AU")).collect()
    //temp.foreach(println)

    //println(content.take(20))
    val content_modified=content.map(x => (x(5),1))

    val local_mapping=Array(("DE","Germany"), ("US","United States"), ("CN", "China"), ("IN","India"))
    val mapping = sc.parallelize(local_mapping)

    //content_modified.join(mapping).takeSample(false, 5, seed=1).foreach(println)
    //content_modified.leftOuterJoin(mapping).takeSample(false, 5, seed=1).foreach(println)

    println(content.getStorageLevel)
    content.persist()
    println(content.getStorageLevel)
    content.unpersist()
    println(content.getStorageLevel)


}
