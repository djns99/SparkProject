import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object Test {

  def pathShorter(x: (VertexId, ShortestPaths.SPMap)): Int = {
    // Sort by nodes closest to specified node
    if (x._2.isEmpty) {
      return Int.MaxValue
    }
    x._2.minBy(path => path._2)._2
  }

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
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val vertexDF = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(StructType(Array(StructField("page_id", LongType), StructField("page_title", StringType))))
      .load(args(0))
      // Lower case titles
      .withColumn("page_title", lower(col("page_title")))
      .toDF
    vertexDF.persist()
    val vertexRDD = vertexDF.rdd.map(x => (x(0).asInstanceOf[VertexId], x(1).asInstanceOf[String]))

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

    val fname = "felicity"
    val lname = "stokes"

    val partialMatches = vertexDF.filter($"page_title".rlike("(^|_)" + fname + "($|_)") || $"page_title".rlike("(^|_)" + lname + "($|_)"))
    partialMatches.persist()
    partialMatches.show(10)
    val imperfectFullMatches = partialMatches.filter($"page_title".rlike("(^|_)" + fname + "(_.*)??_" + lname + "($|_)"))
    imperfectFullMatches.persist()
    imperfectFullMatches.show()
    val perfectMatches = imperfectFullMatches.filter($"page_title".rlike("(^|_)" + fname + "_" + lname + "($|_)"))
    perfectMatches.persist()
    perfectMatches.show()

    graph.connectedComponents().vertices.foreach(System.out.println)

    val felicities = perfectMatches.select("page_id").rdd.map(x => x(0).asInstanceOf[VertexId]).collect().toSeq
    felicities.foreach(System.out.println)
    val pathGraph = ShortestPaths.run(graph, felicities)
    pathGraph.vertices.sortBy(pathShorter).foreach(x => {
      for( path <- x._2 ) {
        System.out.println(x._1 + " is " + path._2 + " steps away from " + path._1)
      }
    })
    //    pathGraph.edges.foreach(System.out.println)
    //    pathGraph.vertices.foreach(System.out.println)
  }
}
