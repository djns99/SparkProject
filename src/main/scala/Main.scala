import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]) {

    if(args.length < 2 || args.length > 3) {
      System.err.println("Usage: java Main <page titles file> <page links file> <(optional) spark scratch dir>")
      System.exit(1)
    }

    var sparkSession = SparkSession
      .builder()
      .appName("Spark Project")
      .config("spark.master", "local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if(args.length == 3) {
      sparkSession = sparkSession.config("spark.local.dir", args(2))
    }

    val spark = sparkSession.getOrCreate()

    import spark.implicits._

    val vertexDF = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(StructType(Array(StructField("page_id", IntegerType), StructField("page_title", StringType))))
      .load(args(0))
      // Lower case titles
      .withColumn("page_title", lower(col("page_title")))
      .toDF
    vertexDF.persist()
    val vertexRDD = vertexDF.rdd.map(x => (x(0).asInstanceOf[String].toLong, x(1).asInstanceOf[String]))

    val partitions = 1
    val edgeGraph = GraphLoader.edgeListFile(spark.sparkContext, args(1), false, partitions)
    val graph = Graph(vertexRDD, edgeGraph.edges)

    graph.persist()

    val ranked = graph.pageRank(0.0000001)
    val rankedNeighbours = ranked.collectNeighbors(EdgeDirection.Either)
    rankedNeighbours.sortByKey().foreach(x => {
      System.out.println(x._1)
      val sorted = x._2.sortBy(x => x._2)
      for( neighbour <- sorted ) {
        System.out.println("'- " + neighbour._1 + ": " + neighbour._2)
      }
    })

    val fname = "page0"
    val lname = "stokes"

    val partialMatches = vertexDF.filter($"page_title".rlike(".*(^|_)" + fname + "($|_).*") || $"page_title".rlike(".*(^|_)" + lname + "($|_).*"))
    partialMatches.persist()
    partialMatches.show(10)
    val imperfectFullMatches = partialMatches.filter($"page_title".rlike(".*(^|_)" + fname + "(_.*_|_)" + lname + "($|_).*"))
    imperfectFullMatches.persist()
    imperfectFullMatches.show()
    val perfectMatches = imperfectFullMatches.filter($"page_title".rlike(".*(^|_)" + fname + "_" + lname + "($|_).*"))
    perfectMatches.persist()
    perfectMatches.show()
  }
}
