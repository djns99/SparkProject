import java.io.BufferedReader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

import scala.util.control._
import scala.io.StdIn
import scala.util.parsing.input.StreamReader
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object Main {

  private var titleFile: String = null
  private var edgesFile: String = null

  private var sparkSessionBuilder: org.apache.spark.sql.SparkSession.Builder = null
  private var spark: SparkSession = null

  private var vertexDF: DataFrame = null
  private var vertexRDD: RDD[(VertexId,String)] = null

  private var graph: Graph[String, Int] = null
  private var personalisedPageRankID: Long = -1
  private var personalisedPageRank: Graph[Double, Double] = null
  private var pageRank: Graph[Double, Double] = null
  private var stronglyConnectedComponents: Graph[VertexId, Int] = null
  private var neighboursRDD: RDD[(VertexId, Array[(VertexId, String)])] = null

  def prompt(msg: String): Unit = {
    System.out.print(Console.GREEN + msg + " > " + Console.RESET)
    System.out.flush()
  }

  def error(msg: String): Unit = {
    System.out.println(Console.RED + msg + Console.RESET)
  }

  def getYN(msg: String): Boolean = {
    do {
      prompt(msg + " (y/n)")
      val answer = StdIn.readLine
      if(answer.equals("y") || answer.equals("yes")) {
        return true
      } else if(answer.equals("n") || answer.equals("no")){
        return false
      }
      error("Please provide a yes or no answer")
    }while(true)
    false
  }

  def getDouble(msg: String): Double = {
    do {
      prompt(msg)
      val answer = StdIn.readLine
      try {
        return answer.toDouble
      }
      catch {
        case nfe: NumberFormatException => {
          error("Please provide a valid double")
        }
        case e: Exception => {
          error("Failed to parse")
        }
      }
    }while(true)
    0.0
  }

  def getLong(msg: String): Long = {
    do {
      prompt(msg)
      val answer = StdIn.readLine
      try {
        return answer.toLong
      }
      catch {
        case nfe: NumberFormatException => {
          error("Please provide a valid integer")
        }
        case e: Exception => {
          error("Failed to parse")
        }
      }
    }while(true)
    0L
  }

  def genPageRank(nodeID: Option[Long]): Boolean = {
    if(nodeID.isEmpty && pageRank != null){
      // No need to rerun already generated overall pageRank
      val confirm = getYN("Do you want to rerun general page rank")
      if(!confirm) {
        System.out.println("Skipped page rank")
        return false
      }
    }

    if(nodeID.isDefined) {
      System.out.println("Running personalised page rank")
      personalisedPageRankID = nodeID.get
    } else {
      System.out.println("Running general page rank")
    }

    try {
      val quickstart = !getYN("Do you wish to customise algorithm ")
      if (quickstart) {
        System.out.println("Running page rank algorithm")
        if(nodeID.isDefined) {
          personalisedPageRank = graph.personalizedPageRank(nodeID.get, 0.001)
        } else {
          pageRank = graph.pageRank(0.001)
        }
        System.out.println("Completed page rank algorithm")
        return true
      }
      val static = getYN("Do you wish to run fixed iteration page rank (default dynamic)?")
      if (static) {
        val iters = getLong("Enter the number of iterations to run")
        System.out.println("Running page rank algorithm")
        if(nodeID.isDefined) {
          personalisedPageRank = graph.staticPersonalizedPageRank(nodeID.get, iters.asInstanceOf[Int])
        } else {
          pageRank = graph.staticPageRank(iters.asInstanceOf[Int])
        }
        System.out.println("Completed page rank algorithm")
      } else {
        val convergence = getDouble("Enter the convergence target")
        System.out.println("Running page rank algorithm")
        if(nodeID.isDefined) {
          personalisedPageRank = graph.personalizedPageRank(nodeID.get, convergence)
        } else {
          pageRank = graph.pageRank(convergence)
        }
        System.out.println("Completed page rank algorithm")
      }
      true
    }catch {
      case e:Exception => {
        error("Failed to run page rank")
        false
      }
    }
  }

  def loadTitles(filePath: String): DataFrame = {
    val vertexDF = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(StructType(Array(StructField("page_id", LongType), StructField("page_title", StringType))))
      .load(filePath)
      // Lower case titles
      .withColumn("page_title", lower(col("page_title")))
      .toDF
    vertexDF.persist()
    vertexDF
  }

  def getTitleFile: Boolean = {
    try{
      prompt("Please enter titles file")

      titleFile = StdIn.readLine().trim
      if (titleFile.equals("help") || titleFile.equals("h")) {
        error("File must contain two tab separated fields.\n" +
          "The first of which is an integer representing the page id and the second is the title\n" +
          "The file should not include a header")
        return false
      }

      System.out.println("Loading " + titleFile + "...\nThis may take a while")
      vertexDF = loadTitles(titleFile)
      vertexRDD = vertexDF.rdd.map(x => (x(0).asInstanceOf[VertexId], x(1).asInstanceOf[String]))
      System.out.println("Titles loaded. Found " + vertexDF.count() + " different pages")

      return true
    } catch {
      case e: Exception => {
        error("Failed to load titles from file. Please make sure the file exists and is in the correct format\nType help for more information\n" + e.getMessage)
        // Null graph so GC can clean it
        graph = null
      }
    }
    false
  }

  def getEdgesFile: Boolean = {
    try {
      prompt("Please enter page links file")
      edgesFile = StdIn.readLine().trim
      System.out.println("Loading " + edgesFile + "...\nThis may take a while")

      if (edgesFile.equals("help") || edgesFile.equals("h")) {
        error("File must contain two tab separated integers.\n" +
          "The first is the source page id, the second is the destination page id\n" +
          "The file may include lines commented with #")
        return false
      }

      val partitions = 1
      val edgeGraph = GraphLoader.edgeListFile(spark.sparkContext, edgesFile, canonicalOrientation = false, partitions)
      graph = Graph(vertexRDD, edgeGraph.edges)
      pageRank = null
      stronglyConnectedComponents = null
      personalisedPageRank = null
      personalisedPageRankID = -1
      neighboursRDD = null

      System.out.println("Links loaded. Found " + graph.edges.count() + " links")

      return true
    } catch {
      case e: Exception => {
        error("Failed to load links from file. Please make sure the file exists and is in the correct format\nType help for more information\n" + e.getMessage)
        // Null graph so GC can clean it
        graph = null
      }
    }
    false
  }

  def loadNewGraph(): Unit = {
    while(!getTitleFile) {}
    while (!getEdgesFile) {}
  }

  def getMatchingNames(fname: String, lname: String, threshold: Int = 3): DataFrame = {
    val partialMatches = vertexDF.filter(col("page_title").rlike("(^| )" + fname + "($| )") || col("page_title").rlike("(^| )" + lname + "($| )"))
    partialMatches.persist()
    val imperfectFullMatches = partialMatches.filter(col("page_title").rlike("(^| )" + fname + "( .* | )" + lname + "($| )"))
    imperfectFullMatches.persist()
    val perfectMatches = imperfectFullMatches.filter(col("page_title").rlike("(^| )" + fname + " " + lname + "( |$)"))
    perfectMatches.persist()

    // Always have at least three matches
    if(perfectMatches.count() >= threshold) {
      imperfectFullMatches.unpersist()
      partialMatches.unpersist()
      perfectMatches
    } else if(imperfectFullMatches.count() >= threshold) {
      perfectMatches.unpersist()
      partialMatches.unpersist()
      imperfectFullMatches
    } else if(partialMatches.count() != 0) {
      perfectMatches.unpersist()
      imperfectFullMatches.unpersist()
      partialMatches
    } else {
      System.out.println("No matches!?! Go out there and get a wikipedia page for yourself!!!")
      null
    }
  }

  def rankMe(): Unit = {
    // Import implicts
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val break = new Breaks
    do {
      break.breakable {
        prompt("Enter the name you would like to search")

        val name = StdIn.readLine().toLowerCase
        val names = name.split(" ")
        if (names.length == 2) {
          val matchingDF = getMatchingNames(names(0), names(1))

          if (matchingDF == null || matchingDF.count() == 0) {
            val tryAgain = getYN("Didn't find a matching name. Try again with a different name?")
            if(!tryAgain) {
              return
            }
            break.break
          }

          var topThree = matchingDF.select("page_id").map(x => x(0).asInstanceOf[Long]).collect()
          if(topThree.length > 3) {
            val ids = topThree.toSet

              System.out.println("Too many matches, doing an initial rank to find best candidates")
            // Run general page rank
            genPageRank(None)
            topThree = pageRank.vertices.filter(x => ids.contains(x._1)).sortBy(x => x, ascending = false).take(3).map(x => x._1)
          }

          System.out.println("Found " + topThree.length + " candidates. Running personalised rank")

          val candidateNames = matchingDF.filter(col("page_id").isInCollection(topThree)).map(x => (x(0).asInstanceOf[Long], x(1).asInstanceOf[String])).collect().toMap
          var bestCandidate = 0L
          var bestCandidateScore = 0.0
          for( candidate <- topThree ) {
            System.out.println("Investigating candidate: " + candidateNames(candidate))
            genPageRank(Option(candidate))
            var rank = personalisedPageRank.vertices.filter(x => x._1 == candidate).take(1)(0)._2
            if(bestCandidateScore < rank) {
              bestCandidateScore = rank
              bestCandidate = candidate
            }
          }

          System.out.println("The most famous person with a similar name to you is: " + candidateNames(bestCandidate) + " (id: " + bestCandidate.toString + ")")

          val tryAgain = getYN("Run again with a different name?")
          if(!tryAgain) {
            return
          }
        } else {
          error("Please enter a single first and last name separated by space")
        }
      }
    } while (true)
  }

  def nearestNeighbourTopicSearch(matchingTopics: Map[VertexId, String]): Unit = {
    // Import implicts
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    if (neighboursRDD == null) {
      System.out.println("Getting neighbouring topics")
      val outRDD = graph.collectNeighbors(EdgeDirection.Out)
      val inRDD = graph.collectNeighbors(EdgeDirection.In)
      // Get bidirectional links - more likely to indicate relation ship
      neighboursRDD = inRDD.join(outRDD).map(x => (x._1, x._2._1.intersect(x._2._2)))
      neighboursRDD.persist()
    }

    // Generate page rank to rank neighbouring topics
    genPageRank(None)
    val rankMap = pageRank.vertices.collect().toMap

    System.out.println("Processing neighbouring topics")

    val neighbouringTopics = neighboursRDD.filter(x => matchingTopics.contains(x._1))
    neighbouringTopics.persist()
    if (neighbouringTopics.count() == 0) {
      System.out.println("Didn't find any neighbouring topics")
    } else {
      neighbouringTopics.foreach(neighbours => {
        if (neighbours._2.isEmpty) {
          System.out.println("No closely related topics to " + matchingTopics(neighbours._1))
        } else {
          System.out.println("Topics related to " + matchingTopics(neighbours._1))
          neighbours._2.sortBy(x => rankMap.get(x._1))
          for (topic <- neighbours._2) {
            System.out.println(topic._2)
          }
        }
      })
    }
    neighbouringTopics.unpersist()

  }

  def connectedTopicSearch(matchingTopics: Map[VertexId, String]): Unit = {
    if(stronglyConnectedComponents == null) {
      System.out.println("Generating strongly connected components.")
      val iters = getLong("Number of iterations to run")
      stronglyConnectedComponents = graph.stronglyConnectedComponents(iters.asInstanceOf[Int])
    }

    // Generate page rank to rank connected topics
    genPageRank(None)
    val rankMap = pageRank.vertices.collect().toMap

    val connectedComponents = stronglyConnectedComponents.vertices.filter(x => matchingTopics.contains(x._1)).map(x => x._2).collect().toSet

    val relatedTopics = stronglyConnectedComponents.vertices.filter(x => connectedComponents.contains(x._2)).sortBy(x => rankMap.get(x._1))
    val numTopics = getLong("How many topics do you want?")

    val selectedTopics = relatedTopics.takeOrdered(numTopics.asInstanceOf[Int]).map(x => x._1).toSet

    // Print names
    graph.vertices.filter(x => selectedTopics.contains(x._1)).map(x => x._2).foreach(System.out.println)
  }

  def whereToNext(): Unit = {
    // Import implicts
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val deep = getYN("Would you like to run the deep topic search (default nearest neighbour)")
    do {
      prompt("Enter topic name")
      val topic = StdIn.readLine().trim.toLowerCase()

      val matchingTopics = vertexDF.filter(col("page_title").contains(topic)).select("page_id", "page_title").as[(VertexId, String)].collect().toMap

      if(matchingTopics.nonEmpty) {
        if(deep)
        {
          connectedTopicSearch(matchingTopics)
        } else {
          nearestNeighbourTopicSearch(matchingTopics)
        }
      } else {
        System.out.println("Didn't find any topics matching " + topic)
      }

      val tryAgain = getYN("Run again for a different topic?")
      if(!tryAgain) {
        return
      }
    }while(true)
  }

  def matchMe(): Unit = {
    // Import implicts
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val break = new Breaks
    do {
      break.breakable {
        prompt("Enter your name")


        val yourName = StdIn.readLine().toLowerCase
        val yourNames = yourName.split(" ")
        if (yourNames.length != 2) {
          error("Please enter a single first and last name separated by space")
          break.break
        }
        val yourMatchingDF = getMatchingNames(yourNames(0), yourNames(1), 1)

        if (yourMatchingDF == null || yourMatchingDF.count() == 0) {
          val tryAgain = getYN("Didn't find a matching name. Try again with a different name?")
          if (!tryAgain) {
            return
          }
          break.break
        }

        var theirMatchingDF: DataFrame = null
        var loop = false
        do {
          loop = false
          prompt("Enter their name")

          val theirName = StdIn.readLine().toLowerCase
          val theirNames = theirName.split(" ")
          if (theirNames.length != 2) {
            loop = true
            error("Please enter a single first and last name separated by space")
          }
          else {
            theirMatchingDF = getMatchingNames(theirNames(0), theirNames(1), 1)

            if (theirMatchingDF == null || theirMatchingDF.count() == 0) {
              val tryAgain = getYN("Didn't find a matching name. Try again with a different name?")
              if (!tryAgain) {
                return
              }
              loop = true
            }
          }
        }while(loop)

        val shortestPaths = ShortestPaths.run(graph, yourMatchingDF.map(x => x(0).asInstanceOf[VertexId]).collect().toSeq)
        val theirIDMap = theirMatchingDF.select($"page_id").as[VertexId].collect().toSet
        val filteredVertexes = shortestPaths.vertices.filter(x => theirIDMap.contains(x._1) && x._2.nonEmpty)
        filteredVertexes.persist()
        if(filteredVertexes.count() == 0) {
          System.out.println("Bad news, wikipedia doesn't think this will work out")
        }
        else {
          val dist = filteredVertexes.map(x => x._2.minBy(x => x._2)._2).min()
          System.out.println("You are " + dist + " steps apart")
        }
        filteredVertexes.unpersist()

        val tryAgain = getYN("Run again with a different name?")
        if (!tryAgain) {
          return
        }
      }
    } while (true)
  }

  def printHelp(): Unit = {
    System.out.println(Console.RED + "Wikipedia relationships explorer help")
    System.out.println("Wikipedia relationships explorer allows you to discover interesting relationships between topics or people")
    System.out.println("\nList of commands:\n\n")
    System.out.println("\trankme | rm            Rank me. How important are the people who share my name.")
    System.out.println("\tmatchme | mm           Match making! Enter you and another persons name and we'll see how compatible wikipedia thinks you are")
    System.out.println("\twhere2next | w2n       Where to next? This tool can help you find related topics to one you are interested in")
    System.out.println("\treload | r             Reload the graph from different files")
    System.out.println("\tquit | q               Quit the program")
    System.out.println("\thelp | h               Print this help page" + Console.RESET)
  }

  def main(args: Array[String]) {

    if (args.length > 1) {
      System.err.println("Usage: java Main <spark scratch dir (optional)>")
      System.exit(1)
    }

    sparkSessionBuilder = SparkSession
      .builder()
      .appName("Spark Project")
      .config("spark.master", "local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if(args.length == 1) {
      sparkSessionBuilder = sparkSessionBuilder.config("spark.local.dir", args(0))
    }

    spark = sparkSessionBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    System.out.println(spark)
    System.out.println("Welcome to wikipedia relationships explorer")
    loadNewGraph()

    while (true) {
      prompt("Enter command")
      val command = StdIn.readLine()

      if(command.equalsIgnoreCase("help") || command.equalsIgnoreCase("h")) {
        printHelp()
      }
      else if (command.equalsIgnoreCase("rankme") || command.equalsIgnoreCase("rm"))
      {
        rankMe()
      }
      else if (command.equalsIgnoreCase("matchme") || command.equalsIgnoreCase("mm"))
      {
        matchMe()
      }
      else if (command.equalsIgnoreCase("where2next") || command.equalsIgnoreCase("w2n")) {
        whereToNext()
      }
      else if (command.equalsIgnoreCase("reload") || command.equalsIgnoreCase("r")) {
        loadNewGraph()
      }
      else if (command.equalsIgnoreCase("quit") || command.equalsIgnoreCase("q")) {
        System.exit(0)
      }
      else {
        System.out.println(Console.RED + "Unrecognised command" + Console.RESET)
        printHelp()
      }

    }

  }
}