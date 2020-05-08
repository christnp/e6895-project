/** Simple Graph Network Exploration using Spark GraphX
  * General Notes:
  *  1. When using a graph multiple times, make sure to call Graph.cache() 
  *     on it first.
  *  2. 
  */
//https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala
// package edu.columbia.advbigdata.preprocess

// spark-core_2.11 artifacts
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

// spark-sql_2.11 artifacts
import org.apache.spark.sql.functions._ //{col,udf,size,asc,monotonically_increasing_id}
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.{DataFrame,Row,Column}
// import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

// spark-bigquery_2.11 artifacts
import com.google.cloud.spark.bigquery._
// import com.google.auth.oauth2.GoogleCredentials

// spark-graphx_2.11 artifacts
import org.apache.spark.graphx._

// graphframes_2.11 artifact
import org.graphframes._

// neo4j-spark-connector artifact
import org.neo4j.spark._

// org.scala-lang artifacts
import scala.util.Random 
import scala.collection._

// slf4j-api artifact
import org.slf4j.{LoggerFactory,Logger}
import java.time.{Clock, Instant}
import java.time._
import java.time.zone.ZoneRulesProvider
import java.time.format._


// project specific packages
import edu.columbia.advbigdata.utils.sparkutils._
import edu.columbia.advbigdata.utils.helpers._


// Test data: file:///home/christnp/Development/e6895/data/IPIP-FFM-data-8Nov2018/data-small.csv
/**
 * Factory for [[graph.SimpleNetwork]] 
 * 
 */
object SimpleNetwork {
  
  def logger : Logger = LoggerFactory.getLogger( DatasetEmulator.getClass )

  private val prgName = "SimpleNetwork"
  
  // Google BQ variables
  val GCP_BUCKET = "eecs-e6895-bucket"
  val GCP_PROJECT = "eecs-e6895-edu"
  val BQ_DATASET = "emu_dataset"
  // val BQ_TABLE = "emudata_2"
  val BQ_TABLE = "ipip120_10000"

  /** requires 2 input files to build network */
  def main(args: Array[String]) 
  {

    val start: Instant = Instant.now() //.now(fixedClock)
    logger.info(s"Starting application '${prgName}' at $start.")

    // Google StackDriver logger
    // https://googleapis.dev/python/logging/latest/usage.html#export-log-entries-using-sinks
    // https://cloud.google.com/logging/docs/api/tasks/exporting-logs#logging-create-sink-java

    // neo4j parameters
    var neo4j_inst = "localhost"
    var neo4j_user = "neo4j"
    var neo4j_pass = "neo4j"
    // check arg input
    var DEBUG = 0
    var _set_size = 0.0
    var csvFile = ""
    if (args.length < 1) 
    {
      throw new IllegalArgumentException(
        "At least 1 input (size of data) is required: <_set_size> <IPIP_CSV_PATH[optional]>")
    } 
    else if (args.length > 1) 
    {
       csvFile = args(0)
      DEBUG = 1
      neo4j_inst = "localhost"
      neo4j_user = "neo4j"
      neo4j_pass = "admin"
      
      logger.info(s"Mode: DEBUG\n")
      logger.info(s"csvFile " + csvFile)
      // logger.info(s"Mode: DEBUG")
    }
    else 
    {
        logger.info(s"Mode: RELEASE")
      // logger.info(s"Mode: RELEASE")
    }

    if(!args(0).isEmpty()) 
    {
      _set_size = args(0).toDouble // determines how big the final dataset is, 0 to use entire dataset
    }

    logger.info(s"Starting " + prgName + " now...")
     /** create the SparkSession
     */
    val spark = SparkSession.builder()
      // .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.14.0-beta") // Spark-BQ connector >doesn't seem to do anything
      // .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery_2.11:0.14.0-beta") // Spark-BQ connector >doesn't seem to do anything
      // .config("spark.jars.packages", "graphframes:graphframes:0.8.0-spark2.4-s_2.11") 
      .config("spark.neo4j.bolt.url", "bolt://"+neo4j_inst+">:7687")
      .config("spark.neo4j.bolt.user", neo4j_user)
      .config("spark.neo4j.bolt.password", neo4j_pass)
      // .master("local[*]") //"Spark://master:7077"
      .appName(prgName)
      .getOrCreate()
    // the SparkContext for later use
    val sc = spark.sparkContext
    // import implicits or this session
    import spark.implicits._
    /**/

    /** Get some cluster specifications and set some parameters
     */
    // get the total number of cores in the cluster
    // ref: https://kb.databricks.com/clusters/calculate-number-of-cores.html
    var workerNodes = sc.statusTracker.getExecutorInfos.length - 1 // minus 1 to remove driver node //sc.defaultParallelism()
    if(workerNodes == 0) workerNodes = 1 // probably running locally
    
    var coresPerNode = java.lang.Runtime.getRuntime.availableProcessors
    if(coresPerNode == 0) throw new Exception(s"Invalid number of cores per node: $coresPerNode");

    var coresPerCluster = workerNodes * coresPerNode 

    logger.info(s"Cluster Specifications:")
    logger.info(s"workerNodes: $workerNodes")
    logger.info(s"coresPerNode: $coresPerNode")
    logger.info(s"coresPerCluster: $coresPerCluster")
    // Partitions should be 3 to 4 times the number of cores in the cluster
    // ref: https://stackoverflow.com/questions/35800795/number-of-partitions-in-rdd-and-performance-in-spark
    val numPartitions = 4 * coresPerCluster
    logger.info(s"Calculated numPartitions: $numPartitions")

     /** read in the dataframe to Google Big Query table
     *  - be sure to identify depenendecy in Maven
     *  - be sure to add connector config to SparkSession
     *  - be sure to "mvn install" an uberJar
     */
    var dinDF = spark.emptyDataFrame
    if (DEBUG > 0) 
    {
      // the debug CSV has cols(<Array>) as cols(<String>) because Spark
      // cannot write <Array> type to CSV. Let's back that out here...
      dinDF = dinDF.readFromCSV(csvFile)
        .withColumn("goals", split(col("goals_str"), "\\|").cast("array<String>"))
        .drop("goals_str")
            logger.info(s"CSV file '" + csvFile + "' read into DataFrame...")
      dinDF.show()
    }
    else 
    {
      val gbq_table = GCP_PROJECT+":"+BQ_DATASET+"."+BQ_TABLE
      logger.info(s"Reading data from Google BQ table '${gbq_table}'...")
      // read in the BQ table as the master DF and drop NaN/Null
      dinDF = dinDF.readFromBQ(GCP_BUCKET,gbq_table).na.drop()
        .repartition(numPartitions)
      //       logger.info(s"BQ Table read into DataFrame...")
      logger.info(s"Data successfully read into Spark DataFrame!")
    }

    // for testing, limit to _set_size records
    if(_set_size > 0.0)
    {
      dinDF = dinDF.limit(_set_size.toInt)
      logger.info(s"Rows in sampled DF: ${dinDF.count()}")
    }

    dinDF = dinDF.orderBy(asc("uuid"))


    logger.info(s"Number of rows =  ${dinDF.count()}")
    logger.info(s"Number of partitions =  ${dinDF.rdd.getNumPartitions}")

    // common columns
    val colComm = Seq("uuid","name","date","age","sex","country","goals") // for now, include goals as attributes
  
    /** create Openness Simple Graph, grOpn */
    val colOpn = colComm ++ Seq("OPN_Z")
    // (1) establish the Openness vertex DF from the master DF
    var opnDF_v = dinDF.select(colOpn.head, colOpn.tail: _*)
      .repartition(numPartitions)
    // force persist
    opnDF_v.persist(StorageLevel.MEMORY_AND_DISK).count() // MEMORY_AND_DISK lessens burden on Memory, adds a little CPU overhead

    // create an ID (uuid is the "name"/node)
    opnDF_v = opnDF_v.withColumnRenamed("uuid", "id") // rename the UUID column as ID for GraphFrames
    ////////////// deprecated
    // opnDF_v = opnDF_v.withColumn("id", monotonically_increasing_id())
    //   .select("id", opnDF_v.columns:_*)
    /////////////////////////

    logger.info(s"opnDF_v")
    opnDF_v.show(5)   

    // statistics on the data
    val opnMean = opnDF_v.select(mean("OPN_Z")).collect.head.getDouble(0)
    val opnStddev = opnDF_v.select(stddev("OPN_Z")).collect.head.getDouble(0)

    logger.info(s"opnMean = ${opnMean.toString()}")
    logger.info(s"opnStddev = ${opnStddev.toString()}")


    // (2) create two temporary DFs for edges ("src","OPN_src","uuid_dest","OPN_dst")
    val colFeat_e = Seq("OPN_src","OPN_dst")
    //     avoid column name collisions
    // val colTemp = Seq("uuid","OPN_Z")
    // var tempDF_src = opnDF_v.select($"uuid".as("src"),$"OPN_Z".as(colFeat_e(0))) // NC: with new dataset, using UUID as id and NAME as src/dst
    // var tempDF_dst = opnDF_v.select($"uuid".as("dst"),$"OPN_Z".as((colFeat_e(1))))
    var tempDF_src = opnDF_v.select($"id".as("src"),$"OPN_Z".as(colFeat_e(0)))
    var tempDF_dst = opnDF_v.select($"id".as("dst"),$"OPN_Z".as((colFeat_e(1))))

    // (3) create the edge dataframe of [all connections,connections > opnThresh]
    var opnDF_e = tempDF_src.repartition(numPartitions)
    // force persist
    opnDF_e.persist(StorageLevel.MEMORY_AND_DISK).count() // MEMORY_AND_DISK lessens burden on Memory, adds a little CPU overhead

    // Z-score values tells us how far above (+) or below (-) the mean population
    // score a user is; therefore, if we want users mean scores that are at or 
    // above 1-sigma, then our threshold for the mean values would be 1
    // val upperBound = opnMean+1*opnStddev // for now, the bound is +/- 1-sigma (or 68%, if we assume normal distribution)
    // val lowerBound = opnMean-1*opnStddev
    val zThresh = 1.0 //only connect users if their sample mean of z-scores is greater than 1-sigma z-score
    opnDF_e = opnDF_e.join(tempDF_dst,
        (tempDF_src("src") !== tempDF_dst("dst")) &&                            // don't allow loopback, nodes linked to self 
      (((tempDF_src(colFeat_e(0))+tempDF_dst(colFeat_e(1)))/2 >= zThresh))
        // (abs(tempDF_src(colFeat_e(0))-tempDF_dst(colFeat_e(1))) <= upperBound) && // only connect users within the bounds
        // (abs(tempDF_src(colFeat_e(0))-tempDF_dst(colFeat_e(1))) >= lowerBound) 
       // only connect users that are 2sigma within the population mean
      //   (abs(tempDF_src(colFeat_e(0))-tempDF_dst(colFeat_e(1)))/2 >= opnMean) // only connect if mean distance between user scores is greater than population mean
      // (((tempDF_src(colFeat_e(0))+tempDF_dst(colFeat_e(1)))/2 >= opnMean)))  
    ).repartition(numPartitions)

    opnDF_e = opnDF_e.withColumn("OPN_dist",
      (abs(opnDF_e(colFeat_e(0))-opnDF_e(colFeat_e(1))))).drop(colFeat_e:_*)
    

    logger.info(s"")
    logger.info(s"opnDF_e partitions = ${opnDF_e.rdd.getNumPartitions}")

    opnDF_e.show(5) 

    if(DEBUG > 0) {
      logger.info(s"Edge DataFrame...")
      opnDF_e.show()

    }

    // (4) create the GraphFrame
    var grOpn = GraphFrame(opnDF_v, opnDF_e)
      
    grOpn.persist(StorageLevel.MEMORY_AND_DISK) 
    // this persists GraphFrame's edge and vertex DFs. TODO: is this necessary
    // since I've already persisted both DFs?

    // if(DEBUG > 0) {
      logger.info(s"grOpn.vertices")
      grOpn.vertices.show()
      logger.info(s"grOpn.vertices.count = " + grOpn.vertices.count + "\n")

      logger.info(s"grOpn.edges")
      grOpn.edges.show()
      logger.info(s"grOpn.edges.count = " + grOpn.edges.count + "\n")

      logger.info(s"grOpn.country == 'ZAF'")
      grOpn.vertices.filter("country == 'ZAF'").show()
    // }
    
    // (5) Store on neoj4
    // https://github.com/neo4j-contrib/neo4j-spark-connector/blob/master/src/test/scala/org/neo4j/spark/Neo4jSparkTest.scala
    // spark.neo4j.bolt.password=<password>
    // TODO:
    // looks like I need to load data from neo4j, vice storing it there
    // val DF = loader.loadDataFrame
    // val sc = spark.sparkContext
    // val neo = Neo4j(sc)
    // if (DEBUG > 0) {
    //   // if DEBUG, connect locally
    //   // Dummy Cypher query to check connection
    //   // val testConnection = neo.cypher("MATCH (n) RETURN n;").loadRdd[Long]
    //   //       logger.info(s"testConnection="+testConnection)
    //   // val graphFramePair: GraphFrame = GraphFrame(vertices, edges)  
    //   // val frameToGraphx = grOpn.toGraphX.neo.saveGraph(sc, "Test")
    //   val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
    //   val graph = Graph.fromEdges(edges,13L)
    //   Neo4jGraph.saveGraph(sc,graph,"value",("FOOBAR","test"),Option("Foo","id"),Option("Bar","id"),merge = true)
    //   // Neo4jGraph.saveGraph(sc,grOpn.toGraphX,("test1","test2"))

    //   // val Neo4jDataFrame.mergeEdgeList(
    //   //     opnDF_e,                              // Dataframe 
    //   //     ("src", Seq("src")),                
    //   //     ("OPN_mean", Seq("OPN_mean")),
    //   //     ("dst", Seq("dst"))
    //   //   )
    // }
    // else {
    //   println("RELEASE")
    // }

    // (6) analysis
    // Run PageRank until convergence to tolerance "tol".
    // val prOpn = grOpn.pageRank.resetProbability(0.15).tol(0.01).run()
    // Run PageRank for a fixed number of iterations.
    var prOpn = grOpn.pageRank.resetProbability(0.15).maxIter(10).run()
    // Run PageRank personalized for vertex "1" 
    // var prOpn = grOpn.pageRank.resetProbability(0.15).maxIter(10).sourceId("1").run()
    // Run PageRank personalized for vertex ["1", "2", "3", "4"] in parallel 
    // var prOpn = grOpn.parallelPersonalizedPageRank.resetProbability(0.15).maxIter(10).sourceIds(Array("1", "2", "3", "4")).run()

    logger.info(s"Opn pageRank results")
    prOpn.vertices.show()
    prOpn.vertices.select("id", "pagerank").orderBy(desc("pagerank")).show()
    prOpn.edges.select("src", "dst", "weight").orderBy(desc("weight")).show() 


    // for connected components, we need a Spark checkpoint directory
    sc.setCheckpointDir("gs://eecs-e6895-bucket/tmp/")
    var ccOpn = grOpn.connectedComponents.run() 
    ccOpn.select("id", "component").orderBy(desc("component")).show()
    
    
    //////////////////////////////// DEBUG /////////////////////////////////////
    // var dbgDF = spark.emptyDataFrame

    // debug DF
    // val dbgDF = Seq(
    //   (8, "bat"),
    //   (64, "mouse"),
    //   (-27, "horse")
    // ).toDF("number", "word")
    // dbgDF.printGoals(Seq("word"))

    ////////////////////////////////////////////////////////////////////////////

    // close the SparkSession
    // println("Closing SparkSession [" + spark.sparkContext.applicationId + "]")
    // spark.close()
  }
}