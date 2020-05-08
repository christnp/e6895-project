// /** Neo4j Configuration and Testing
//   */

// // spark-core_2.11 artifacts
// import org.apache.spark.rdd.RDD
// import org.apache.spark.storage.StorageLevel

// // spark-sql_2.11 artifacts
// import org.apache.spark.sql.functions._ //{col,udf,size,asc,monotonically_increasing_id}
// import org.apache.spark.sql.{SparkSession,SaveMode}
// import org.apache.spark.sql.{DataFrame,Row,Column}
// // import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

// // spark-bigquery_2.11 artifacts
// import com.google.cloud.spark.bigquery._
// // import com.google.auth.oauth2.GoogleCredentials

// // spark-graphx_2.11 artifacts
// import org.apache.spark.graphx._

// // graphframes_2.11 artifact
// import org.graphframes._

// // neo4j-spark-connector artifact
// import org.neo4j.spark._

// // org.scala-lang artifacts
// import scala.util.Random 
// import scala.collection._

// // slf4j-api artifact
// import org.slf4j.{LoggerFactory,Logger}
// import java.time.{Clock, Instant}
// import java.time._
// import java.time.zone.ZoneRulesProvider
// import java.time.format._

// // project specific packages
// import edu.columbia.advbigdata.utils.sparkutils._
// import edu.columbia.advbigdata.utils.helpers._


// /**
//  * Factory for [[graph.Neo4JConfig]] 
//  * 
//  */
// object Neo4JConfig {
  
//   def logger : Logger = LoggerFactory.getLogger( Neo4JConfig.getClass )

//   private val prgName = "Neo4JConfig"
  
//   // Google BQ variables
//   val GCP_BUCKET = "eecs-e6895-bucket"
//   val GCP_PROJECT = "eecs-e6895-edu"
//   val BQ_DATASET = "emu_dataset"
//   // val BQ_TABLE = "emudata_2"
//   val BQ_TABLE = "ipip120_10000"

//   /**
//    * Function to ingest a DataFrame with layer column name identified
//    * and return a GraphFrame object. The idea is that multiple layers
//    * can be generated using the same process because each layer utilizes
//    * similar data (i.e., name, uuid, location, etc.). 
//    *
//    *  @param df the source DataFrame
//    *  @param layerCol string column name representing the layer data
//    *  @return a GraphFrame object
//    */
//   def extractLayer(df: DataFrame, layerCol: String, partitions: Int = 0) : Int = {
    
//     logger.info(s"Exporting layer information for '${layerCol}'")

    
//     return 0
//   }
//   /** */
//   def main(args: Array[String]) 
//   {

//     val start: Instant = Instant.now() //.now(fixedClock)
//     logger.info(s"Starting application '${prgName}' at $start.")

//     // neo4j parameters
//     val neo4j_ecrypt = "false"
//     val neo4j_user = "neo4j"
//     var neo4j_pass = "password"
//     var neo4j_host = "34.82.120.135"
//     var neo4j_port = "7687" //bolt port
//     var neo4j_url = s"bolt://${host}:${port}" //"neo4j://localhost"
   
//     // check arg input
//     var DEBUG = 0
//     var _set_size = 0.0
//     var csvFile = ""
//     if (args.length == 0) 
//     {
//         logger.info(s"Mode: RELEASE")
//     } 
//     else if (args.length > 0) 
//     {
//         csvFile = args(0)
//         DEBUG = 1
//         // neo4j_host = "localhost"
//         // // neo4j_port = "7474" //local http port
//         // neo4j_user = "neo4j"
//         // neo4j_pass = "admin"
//         // url = s"bolt://${neo4j_host}:${neo4j_port}" //"neo4j://localhost"
        
//         logger.info(s"Mode: DEBUG")
//     }

//     if (args.length > 1) 
//     {
//         _set_size = args(1).toDouble // determines how big the final dataset is, 0 to use entire dataset
//     }

//     logger.info(s"Starting " + prgName + " now...")
//      /** create the SparkSession
//      */
//     val spark = SparkSession.builder()
//       .config("spark.neo4j.bolt.encryption", neo4j_ecryp)
//       .config("spark.neo4j.bolt.user", neo4j_user)
//       .config("spark.neo4j.bolt.password", neo4j_pass)
//       .config("spark.neo4j.bolt.url", neo4j_url)
//       // .master("local[*]") //"Spark://master:7077"
//       .appName(prgName)
//       .getOrCreate()
//     // the SparkContext for later use
//     val sc = spark.sparkContext
//     // import implicits or this session
//     import spark.implicits._
//     /**/

//     /** Get some cluster specifications and set some parameters
//      */
//     // get the total number of cores in the cluster
//     // ref: https://kb.databricks.com/clusters/calculate-number-of-cores.html
//     var workerNodes = sc.statusTracker.getExecutorInfos.length - 1 // minus 1 to remove driver node //sc.defaultParallelism()
//     if(workerNodes == 0) workerNodes = 1 // probably running locally
    
//     var coresPerNode = java.lang.Runtime.getRuntime.availableProcessors
//     if(coresPerNode == 0) throw new Exception(s"Invalid number of cores per node: $coresPerNode");

//     var coresPerCluster = workerNodes * coresPerNode 

//     logger.info(s"Cluster Specifications:")
//     logger.info(s"workerNodes: $workerNodes")
//     logger.info(s"coresPerNode: $coresPerNode")
//     logger.info(s"coresPerCluster: $coresPerCluster")
//     // Partitions should be 3 to 4 times the number of cores in the cluster
//     // ref: https://stackoverflow.com/questions/35800795/number-of-partitions-in-rdd-and-performance-in-spark
//     val numPartitions = 4 * coresPerCluster
//     logger.info(s"Calculated numPartitions: $numPartitions")

//      /** read in the dataframe to Google Big Query table
//      *  - be sure to identify depenendecy in Maven
//      *  - be sure to add connector config to SparkSession
//      *  - be sure to "mvn install" an uberJar
//      */
//     var dinDF = spark.emptyDataFrame
//     if (DEBUG > 0) 
//     {
//       // the debug CSV has cols(<Array>) as cols(<String>) because Spark
//       // cannot write <Array> type to CSV. Let's back that out here...
//         dinDF = dinDF.readFromCSV(csvFile)
//             .withColumn("goals", split(col("goals_str"), "\\|").cast("array<String>"))
//             .drop("goals_str")
//             .limit(100)
//         logger.info(s"CSV file '" + csvFile + "' read into DataFrame...")
//         dinDF.show()
//     }
//     else 
//     {
//       val gbq_table = GCP_PROJECT+":"+BQ_DATASET+"."+BQ_TABLE
//       logger.info(s"Reading data from Google BQ table '${gbq_table}'...")
//       // read in the BQ table as the master DF and drop NaN/Null
//       dinDF = dinDF.readFromBQ(GCP_BUCKET,gbq_table).na.drop()
//         .repartition(numPartitions)
//       //       logger.info(s"BQ Table read into DataFrame...")
//       logger.info(s"Data successfully read into Spark DataFrame!")
//     }

//     // for testing, limit to _set_size records
//     // TODO: orderBy(asc()) before sampling to make deterministic?
//     if(_set_size > 0.0)
//     {
//       dinDF = dinDF.limit(_set_size.toInt)
//       logger.info(s"Rows in sampled DF: ${dinDF.count()}")
//     }

//     dinDF = dinDF.orderBy(asc("uuid"))



   
//     // // (4) create the GraphFrame
//      /* Export the personality factor layers each as a GraphFrame object
//      *  - it uses the "XXX_Z" input to key the correct layer/column
//      */
//     val grOpn = extractLayer(dinDF,"OPN_Z",numPartitions).persist(StorageLevel.MEMORY_AND_DISK) 
      
//     // this persists GraphFrame's edge and vertex DFs. TODO: is this necessary
//     // since I've already persisted both DFs?

//     // if(DEBUG > 0) {
//     logger.info(s"grOpn.vertices")
//     grOpn.vertices.orderBy(asc("id")).show(10)
//     logger.info(s"grOpn.vertices.count = " + grOpn.vertices.count + "\n")

//     logger.info(s"grOpn.edges")
//     // grOpn.edges.orderBy(desc("OPN_Z_dist")).show()
//     grOpn.edges.orderBy(asc("dst")).show(10)
//     logger.info(s"grOpn.edges.count = " + grOpn.edges.count + "\n")
    
//     // (5) Store on neoj4
//     // https://github.com/neo4j-contrib/neo4j-spark-connector/blob/master/src/test/scala/org/neo4j/spark/Neo4jSparkTest.scala#L140
//     // https://programminghistorian.org/en/lessons/dealing-with-big-data-and-network-analysis-using-neo4j#formatting-csv-files-for-loading-into-neo4j
//     // spark.neo4j.bolt.url="bolt://neo4j:<password>@host:port"
//     // or
//     // spark.neo4j.bolt.url="bolt://host:port"
//     // spark.neo4j.bolt.user="neo4j"
//     // spark.neo4j.bolt.password=<password>
//     // given
   
//     // when

//     // then
//     // Assert.assertEquals(encryption.toBoolean, neo4jConf.encryption)
//     // Assert.assertEquals(user, neo4jConf.user)
//     // Assert.assertEquals(pass, neo4jConf.password.get)
//     // Assert.assertEquals(url, neo4jConf.url)
//     // TODO: looks like I need to load data from neo4j, vice storing it there?
//     // Store data as CSV for loading into neo4j
//     // nodes, id:ID | name:LABEL 
//     // vertices, 
//     val gcp_path = s"gs://${GCP_BUCKET}/output"
//     logger.info(s"Saving data as CSV to $gcp_path")
    
//     // TODO: figure out why goals is in a weird format...
//     // grOpn.vertices.orderBy(asc("id")).withColumn("goals_str", flatten($"goals")).show() // concat_ws(" ", $"rate_plan_code")
//     // grOpn.vertices.orderBy(asc("id")).printSchema() // concat_ws(" ", $"rate_plan_code")
//     // grOpn.vertices.orderBy(asc("id")).withColumn("goals_str", explode($"goals")).select($"goals_str").show() // concat_ws(" ", $"rate_plan_code")

//     // flatten($"subjects")
//     // convert cols(<Array>) to cols(<String>) with "|" delimiter because Spark
//     // cannot write <Array> type to CSV. 
//     logger.info(s"grOpn.write.format('csv')")
//     grOpn.vertices.orderBy(asc("id"))
//         // .withColumn("goals_str", array_join($"goals", "|")) // concat_ws(" ", $"rate_plan_code")
//         // .withColumn("goals_str", concat_ws("|",$"goals")) // concat_ws(" ", $"rate_plan_code")
//         // .withColumn("goals_str", flatten($"goals"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grOpn_vert.csv")

//     grOpn.edges.orderBy(asc("dst"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grOpn_edge.csv")

//     logger.info(s"grCsn.write.format('csv')")
//     grCsn.vertices.orderBy(asc("id"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grCsn_vert.csv")

//     grCsn.edges.orderBy(asc("dst"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grCsn_edge.csv")

//     logger.info(s"grExt.write.format('csv')")
//     grExt.vertices.orderBy(asc("id"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grExt_vert.csv")

//     grExt.edges.orderBy(asc("dst"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grExt_edge.csv")

//     logger.info(s"grAgr.write.format('csv')")
//     grAgr.vertices.orderBy(asc("id"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grAgr_vert.csv")

//     grAgr.edges.orderBy(asc("dst"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grAgr_edge.csv")
    
//     logger.info(s"grNeu.write.format('csv')")
//     grNeu.vertices.orderBy(asc("id"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grNeu_vert.csv")

//     grNeu.edges.orderBy(asc("dst"))
//         .write.format("csv")
//         .option("header", "true")
//         .mode("overwrite")
//         .save(gcp_path+"/grNeu_edge.csv")

//     sys.exit()
//     // val DF = loader.loadDataFrame
//     // val sc = spark.sparkContext
//     // val neo = Neo4j(sc)
//     // if (DEBUG > 0) {
//     //   // if DEBUG, connect locally
//     //   // Dummy Cypher query to check connection
//     //   // val testConnection = neo.cypher("MATCH (n) RETURN n;").loadRdd[Long]
//     //   //       logger.info(s"testConnection="+testConnection)
//     //   // val graphFramePair: GraphFrame = GraphFrame(vertices, edges)  
//     //   // val frameToGraphx = grOpn.toGraphX.neo.saveGraph(sc, "Test")
//     //   val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
//     //   val graph = Graph.fromEdges(edges,13L)
//     //   Neo4jGraph.saveGraph(sc,graph,"value",("FOOBAR","test"),Option("Foo","id"),Option("Bar","id"),merge = true)
//     //   // Neo4jGraph.saveGraph(sc,grOpn.toGraphX,("test1","test2"))

//     //   // val Neo4jDataFrame.mergeEdgeList(
//     //   //     opnDF_e,                              // Dataframe 
//     //   //     ("src", Seq("src")),                
//     //   //     ("OPN_mean", Seq("OPN_mean")),
//     //   //     ("dst", Seq("dst"))
//     //   //   )
//     // }
//     // else {
//     //   println("RELEASE")
//     // }

//     // (6) analysis
//     // Run PageRank until convergence to tolerance "tol".
//     // val prOpn = grOpn.pageRank.resetProbability(0.15).tol(0.01).run()
//     // Run PageRank for a fixed number of iterations.
    
//     // OPN
//      logger.info(s"Starting OPN pageRank")
//     var prOpn = grOpn.pageRank.resetProbability(0.15).maxIter(10).run()
//     prOpn.vertices.orderBy(asc("id")).show(10)
//     prOpn.edges.orderBy(asc("dst")).show(10) 
//     // prOpn.vertices.select("id", "pagerank").orderBy(desc("pagerank")).show()
//     // prOpn.edges.select("src", "dst", "weight").orderBy(desc("weight")).show() 
//     // prOpn.edges.select("src", "dst", "weight").orderBy(asc("dst")).show(10) 
//     // CSN
//     logger.info(s"Starting CSN pageRank")
//     var prCsn = grCsn.pageRank.resetProbability(0.15).maxIter(10).run()
//     prCsn.vertices.orderBy(asc("id")).show(10)
//     prCsn.edges.orderBy(asc("dst")).show(10) 
//     // EXT
//     logger.info(s"Starting EXT pageRank")
//     var prExt = grExt.pageRank.resetProbability(0.15).maxIter(10).run()
//     prExt.vertices.orderBy(asc("id")).show(10)
//     prExt.edges.orderBy(asc("dst")).show(10) 
//     // AGR
//     logger.info(s"Starting AGR pageRank")
//     var prAgr = grAgr.pageRank.resetProbability(0.15).maxIter(10).run()
//     prAgr.vertices.orderBy(asc("id")).show(10)
//     prAgr.edges.orderBy(asc("dst")).show(10) 
//     // NEU
//     logger.info(s"Starting NEU pageRank")
//     var prNeu = grNeu.pageRank.resetProbability(0.15).maxIter(10).run()
//     prNeu.vertices.orderBy(asc("id")).show(10)
//     prNeu.edges.orderBy(asc("dst")).show(10) 

//     // Run PageRank personalized for vertex "1" 
//     // var prOpn = grOpn.pageRank.resetProbability(0.15).maxIter(10).sourceId("1").run()
//     // Run PageRank personalized for vertex ["1", "2", "3", "4"] in parallel 
//     // var prOpn = grOpn.parallelPersonalizedPageRank.resetProbability(0.15).maxIter(10).sourceIds(Array("1", "2", "3", "4")).run()


//     // for connected components, we need a Spark checkpoint directory
//     // sc.setCheckpointDir("gs://eecs-e6895-bucket/tmp/")
//     // logger.info(s"Opn connected components results")
//     // var ccOpn = grOpn.connectedComponents.run() 
//     // // ccOpn.select("id", "component").orderBy(desc("component")).show()
//     // ccOpn.orderBy(asc("id")).show(10)
//     // logger.info(s"Csn conn. comp. results")
//     // var ccCsn = grCsn.connectedComponents.run() 
//     // ccCsn.orderBy(asc("id")).show(10)
    
    
//     //////////////////////////////// DEBUG /////////////////////////////////////
//     // var dbgDF = spark.emptyDataFrame

//     // debug DF
//     // val dbgDF = Seq(
//     //   (8, "bat"),
//     //   (64, "mouse"),
//     //   (-27, "horse")
//     // ).toDF("number", "word")
//     // dbgDF.printGoals(Seq("word"))

//     ////////////////////////////////////////////////////////////////////////////

//     // close the SparkSession
//     // println("Closing SparkSession [" + spark.sparkContext.applicationId + "]")
//     // spark.close()
//   }
// }