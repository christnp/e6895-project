/** Provides a service as described.
  *
  * This is further documentation of what we're documenting.
  * Here are more details about how it works and what it does.
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

// org.scala-lang artifacts
import scala.io.Source
import scala.util.Random 
import scala.collection._

// artifact google-cloud-logging
// import com.google.cloud.logging.LogEntry;
// import com.google.cloud.logging.Logging;
// import com.google.cloud.logging.Logging.EntryListOption;
// import com.google.cloud.logging.LoggingOptions;

// import java.util.logging.Logger;

import org.slf4j.{LoggerFactory,Logger}
import java.time.{Clock, Instant}
import java.time._
import java.time.zone.ZoneRulesProvider
import java.time.format._

import edu.columbia.advbigdata.utils.sparkutils._


// Test data: file:///home/christnp/Development/e6895/data/IPIP-FFM-data-8Nov2018/data-small.csv
/**
 * Factory for [[preprocess.DatasetEmulator]] 
 * 
 */
object DatasetEmulator {
  // setup the logger
  //  private static final Logger logger = Logger.getLogger(DatasetEmulator.class.getName())
  // private val logger = LoggerFactory.getLogger(classOf[DatasetEmulator])

  
  // Logger.info(s"Starting application at $start.")
  // private val logger: Logger = LoggerFactory.getLogger(classOf[BigQueryStreamWriter])
  def logger : Logger = LoggerFactory.getLogger( DatasetEmulator.getClass )

  /**
   * GLOBAL VARIABLES/CONSTANTS
   */
  // Immutable user defined ID column
  val USER_ID_COLUMN : String = "CASE"

  // Five factors in order of the scorecard (global, immutable map):
  // (1) Extraversion -> EXT
  // (2) Agreeableness -> AGR 
  // (3) Conscientiousness -> CSN
  // (4) Neuroticism -> NEU (opposite of Emotional Stability)
  // (5) Openness/Intellect/Imagination -> OPN)
  val factors = Map(
      "Openness" -> "OPN",
      "Conscientiousness" -> "CSN",
      "Extraversion" -> "EXT", 
      "Agreeableness" -> "AGR",
      "Neuroticism" -> "NEU"
  )
//    ID	Key	  Sign  Adjusted  Facet	    Description
//    1	  N1	  +	    True	    Anxiety	  Worry about things.
//    I#  FFM   DNC   Sign DNC  DNC       DNC
// If Adjusted is "True" then no additional processing of raw data is needed.

  // Goal topics (global, immutable map):
  // (1) 'financial', (2) 'spiritual', (3) 'social', (4) 'knowledge', (5) 'personal',
  // (6) 'health', (7) 'adventure', (8) 'career', (9) 'unknown'
  // --> 'Openness' - knowledge,adventure,spiritual,personal,financial,social,career,health
  // --> 'Conscientiousness' - career,financial,personal,health,knowledge,spiritual,adventure,social
  // --> 'Extraversion' - social,adventure,career,personal,financial,spiritual,knowledge,health
  // --> 'Agreeableness' - spiritual,financial,health,social,adventure,knowledge,career,personal
  // --> 'Emotional Stability/Neuroticism' - health,knowledge,personal,spiritual,financial,adventure,career,social,
  // val topics : Map[String,Seq[String]] = Map(
  //                   "financial" -> Seq("AGR","CSN"),
  //                   "spiritual" -> Seq("OPN","AGR"),
  //                   "social" -> Seq("EXT"), 
  //                   "knowledge" -> Seq("OPN","EST"),
  //                   "personal" -> Seq("CSN","EST"),
  //                   "health" -> Seq("EST","AGR"),
  //                   "adventure" -> Seq("OPN","EXT"),
  //                   "career" -> Seq("CSN","EXT"),
  //                   "unknown" -> Seq("OPN","CSN","EXT","AGR","EST")
  //                 )
  val topics : Map[String,Seq[String]] = Map(
      "OPN" -> Seq("knowledge","adventure","spiritual"),
      "CSN" -> Seq("career","financial","personal"),
      "EXT" -> Seq("social","adventure,career"),
      "AGR" -> Seq("spiritual","financial","health"),
      "EST" -> Seq("health","knowledge","personal")
  )
  // Quintile structure (global, mutable Map):
  // changed process in ipipScores(); no longer needed
  // var quint = mutable.Map[String, Seq[Double]]()

  /** Calculates the IPIP FFM scores.
   *
   *  @param ipipDF the Spark DataFrame representing IPIP data
   *  @param scorecardDF the Spark DataFrame representing IPIP scoring details
   *  @param blacklist list of columns to exclude from the dataset (dni raw score data)
   *  @param raw if true, returns raw score data (else removes it)
   *  @param show if true, displays scores to console
   *  @return a Spark DataFrame with the IPIP scores included
   */
  private def ipipScores(
      ipipDF: DataFrame, 
      scorecardDF: DataFrame,
      blacklist: Seq[String] = Seq.empty[String], 
      raw: Boolean = false,
      show: Boolean = false
    ) : DataFrame = {

    // TODO: for now, replace all NONE
    var newDF = ipipDF 
    logger.info(s"ipipScores() -> before scores newDF partitions: ${newDF.rdd.getNumPartitions}")
   
    // if a blacklist is provided, try to remove those columns; also add UUID at front of schema
    if(!blacklist.isEmpty) {
      newDF = newDF.drop(blacklist:_*)
    }

    /** algorithm to calculate scores */
    var factorCols = Seq[String]()
    factors.keys.foreach{ i =>  
        // now using a "scorecard" to generate the factor columns
        factorCols = scorecardDF.filter(col("Key").startsWith(factors(i))).select(col("ID")).rdd.map(r => r(0)).collect.toSeq.asInstanceOf[Seq[String]]
        // factorCols = newDF.columns.toSeq.filter(_.startsWith(factors(i)))
        newDF = newDF.withColumn(factors(i), factorCols.map(c => col(c)).reduce(_ + _)) // NC: changed from "i" to factors(i) (i.e., shortend name)
        // calculate the quintile values for each column (moved)
        // quint += (factors(i) -> newDF.stat.approxQuantile(factors(i),Array(0.20,0.40,0.60,0.80),0.0).toSeq)
       
        // drop the raw data columns, if desired
        if(!raw) newDF = newDF.drop(factorCols:_*)
    }

    logger.info(s"ipipScores() -> summed scores newDF partitions: ${newDF.rdd.getNumPartitions}")


    /** calculate the Z-scores 
     * 
     * to get z-score:    z = (sample - mean)/st_dev
     * to get raw sample: sample = z*st_dev + mean
     * 
     * ref: https://www.simplypsychology.org/z-score.html
     * 
     * notes: moved from "foreach" above
    */
    factors.keys.foreach{ i => 
      // calculate the mean and stddev for z-score calc
      var scoreStats = newDF.select(
          mean(col(factors(i))).alias("mean"),
          stddev(col(factors(i))).alias("stddev")
      ).rdd.map(r=>(r(0),r(1))).first
      // calculate the z-score and add it to the DF
      newDF = newDF.withColumn(factors(i)+"_Z", 
        bround(((expr(factors(i))-scoreStats._1)/scoreStats._2),3)
      )
    }

    logger.info(s"ipipScores() -> z-scores newDF partitions: ${newDF.rdd.getNumPartitions}")


    /** simple UDF to set the quintile values for each column/row
     * 
     *  @param quint sequence representing the quintile values for each column
     * 
     *  @param c Spark DataFrame column for which the quintile is being added
     * 
     */
    def setQuint(quint: Seq[Double]) = udf((c: Int) => {
      var rank : Int = 1
      quint.map { e => if ((c-e) >= 0 ) rank += 1}
      rank
    })
    // 20200411: changed this process, makes order match raw scores
    // quint.keys.foreach{ i =>
    //   newDF = newDF.withColumn(i+"_Q", setQuint(quint(i))(col(i)))
    // }
    factors.keys.foreach{ i => 
      var quint = newDF.stat.approxQuantile(factors(i),Array(0.20,0.40,0.60,0.80),0.0).toSeq
      newDF = newDF.withColumn(factors(i)+"_Q", setQuint(quint)(col(factors(i))))
    }
    logger.info(s"ipipScores() -> quintile scores newDF partitions: ${newDF.rdd.getNumPartitions}")
    
    
    // if(show) {
    //   val f = Seq("uuid") ++ factors.values.toSeq ++ factors.values.toSeq.map(k => k+"_Q") // NC: changed from ".keys" to ".values"" (i.e., shortend name)
    //   newDF.printSchema()
    //   newDF.select(f.head, f.tail:_*).limit(10).show()
    // }

    return newDF    
  }

  /** Assigns goals to users (UUIDs) based on personality scores.
   *
   *  @param idf the Spark DataFrame representing IPIP FFM scores data
   *  @param gdf the Spark DataFrame representing raw goals dataset
   *  @param scores if true, print scores to userconsole
   *  @return a Spark DataFrame with goals assigned to each UUID
   */
  private def datasetEmu(
      idf: DataFrame, 
      gdf: DataFrame, 
      show: Boolean = false
    ) : DataFrame = {

    /** 
     * create user defined function (UDF) with currying to assign goals from list
     * using an algorithm choosing goals based loosely on users' personality
     * @param allGoals a list of tuples representing the goal topic and the goal text
     * @param allTopics: a (k,v) scala.Map where k = goal topic and v = personality assignments
     * @param maxGoals: an integer representing the maximum number of goals per user
     *
     * @param opn the Spark Dataframe column representing Openness
     * @param csn the Spark Dataframe column representing Conscientiousness
     * @param ext the Spark Dataframe column representing Extraversion
     * @param agr the Spark Dataframe column representing Agreeableness
     * @param neu the Spark Dataframe column representing Neuroticism
     */
    // ref 1: https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.functions$
    // ref 2: https://stackoverflow.com/questions/44370479/passing-arguments-to-a-scala-udf
    // register the user defined function with currying to pass list of goals as parameter
    def getGoals(allGoals: List[(Any,Any)], allTopics: Map[String,Seq[String]], maxGoals: Int) = 
      udf((opn: Int, csn: Int, ext: Int, agr: Int, est: Int) => {
        // select applicable goal topics based on personality
        // populate the quintile rank structure
        val theQuints :  Map[String,Int] = Map(
              "OPN" -> opn,
              "CSN" -> csn,
              "EXT" -> ext,
              "AGR" -> agr,
              "EST" -> est
        )
        // iterate over quintile ranks, updating topic list as necessary
        var theTopics = List[String]()
        theQuints.keys.foreach{ i =>  
          if(theQuints(i).toInt > 3) {
            theTopics ++= allTopics(i)
          }
        }
        // TODO: for now, make sure everyone has goals?
        if(theTopics.isEmpty) {
          theTopics ++= allTopics(theQuints.max._1)
        }

        // filter the goals
        val theGoals = allGoals.filter(t => theTopics.toSet.contains(t._1.toString)).map(t => t._2.toString) //.flatMap(t => List(t._2)
        // randomly select some goals from appropriate pool
        val rnd=new Random
        val numGoals = 1 + rnd.nextInt(maxGoals)
        // return      
        Random.shuffle(theGoals).take(numGoals)
      })    
   

    // generate a list of all the goals
    val gList = gdf.rdd.map(r => (r(0),r(1))).collect.toList
    // apply the UDF on the ipip data to assign goals
    logger.info(s"getGoals() -> before goals idf partitions: ${idf.rdd.getNumPartitions}")

    // val newDF = idf.withColumn("goals", getGoals(gList,topics,quint,1)(factors.values.toSeq.map(name => col(name)):_*)) // NC: changed from ".keys" to ".values"" (i.e., shortend name)
    val newDF = idf.withColumn("goals", getGoals(gList,topics,5)(factors.values.toSeq.map(name => col(name+"_Q")):_*)) // NC: changed from ".keys" to ".values"" (i.e., shortend name)
    
    logger.info(s"getGoals() -> with goals newDF partitions: ${newDF.rdd.getNumPartitions}")
    
    // if(show) {
    //   val f = Seq("uuid") ++ factors.values.toSeq ++ factors.values.toSeq.map(k => k+"_Q") ++ Seq("goals")  // NC: changed from ".keys" to ".values"" (i.e., shortend name)
    //   newDF.select(f.head, f.tail:_*).limit(10).show()
    // }

    return newDF 
  }

  /**
   * Helper function to read CSV file into Spark DataFrame. This function will
   * attempt to parse the file for the user defined delimiter first and, if that
   * fails, will attempt to parse using standard comma-delimiter.
   *
   *  @param spark the Spark session for this dataframe
   *  @param csvFile string representing the path to the source CSV file
   *  @param del string representing the delimiter for the CSV file
   *  @param id_column if true, add a column called uuid
   *  @return a Spark DataFrame with raw data
   */
  def readCSV(
      spark: SparkSession, 
      csvFile: String, 
      del: String = ",",
      id_column: String = ""
    ) : DataFrame = {

    var df = spark.emptyDataFrame
    try 
    {
      df = spark.read.format("csv")
            .option("delimiter", del) // ipip seems to be a tab-delimited file, but not if I save it!
            .option("header", "true")
            .option("inferSchema", "true")
            .load(csvFile)
    } 
    catch 
    {        
      case unknown:  Throwable => {
       logger.info(s"Failed to parse CSV with '$del' \n  at $unknown")
        if(del != ",") {
          logger.info(s"Attempting to parse CSV again using ',' ")
          try
          {
            df = spark.read.format("csv")
                      .option("delimiter", ",")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load(csvFile)
          } 
          catch 
          {
            case unknown:  Throwable => logger.error(s"Failed to parse CSV with ','\n  at $unknown")
            logger.error(s">> Closing SparkSession [${spark.sparkContext.applicationId}]")
            spark.stop()
            // exit program
            throw new Exception("Failed to parse CSV file, please verify file input: " + csvFile);
          }
        }
      }
    }

    return df
  }

  /** requires 2 input files to emulate new dataset*/
  def main(args: Array[String]) 
  {
    // val datetime  = LocalDateTime.ofInstant(Instant.now, ZoneId.of("Europe/Helsinki"))
    // val dateClock = LocalDateTime.now(fixedClock)
    // val duration = Duration.between(dateClock, tomorrow)
    val start: Instant = Instant.now() //.now(fixedClock)
    // val start: Int = 100
    logger.info(s"Starting application at $start.")

    // logger.info("Logging INFO with Logback");
    // [START logging_dataset_emulator]
    // LoggingOptions options = LoggingOptions.getDefaultInstance().getService()
    // val logName : String = "dataset-emu-log"

    // val logMe = "This is a test of logging"
    // LogEntry entry = LogEntry.newBuilder(StringPayload.of(logMe))
    //     .setSeverity(Severity.ERROR)
    //     .setLogName(logName)
    //     .setResource(MonitoredResource.newBuilder("global").build())
    //     .build()
    // logging.write(Collections.singleton(entry))
    
    // [END logging_dataset_emulator]

    var DEBUG = 0
    var _set_size = 0.0
    if (args.length < 2) 
    {
      throw new IllegalArgumentException(
        "Exactly 2 input dataset paths are required: <IpipCsvPath> <GoalsCsvPath>")
    } 
    else if (args.length > 2) 
    {
      DEBUG = 1
      if(!args(2).isEmpty()){
        _set_size = args(2).toDouble // determines how big the final dataset is
      }
      logger.info(s"Mode: DEBUG")
    }
    else 
    {
      logger.info(s"Mode: RELEASE")
    }
    // TODO: make the inputs generic?
    // expect the first arg to be the IPIP personality data
    // expect the second arg to be the goals data
    val ipipFile = args(0)
    val goalsFile = args(1)


    /** create the SparkSession
     */
    val spark = SparkSession.builder()
      // .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta") // Spark-BQ connector Doesn't work!? Using --jars in stead
      // .config("spark.jars.packages","gs://spark-lib/bigquery/spark-bigquery-latest.jar")
      .config("spark.sql.debug.maxToStringFields", 200)
      // .master("local[*]") //"Spark://master:7077"
      .appName("DataEmulator")
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
    
    if (sc.getRDDStorageInfo.length > 0) 
    {
      logger.info(s"""RDD information before partition:
                |name: ${sc.getRDDStorageInfo.head.name}
                |numPartitions: ${sc.getRDDStorageInfo.head.numPartitions}
                |numCachedPartitions: ${sc.getRDDStorageInfo.head.numCachedPartitions}
                |memSize: ${sc.getRDDStorageInfo.head.memSize}
                |diskSize: ${sc.getRDDStorageInfo.head.diskSize}
                |diskSize: ${sc.getRDDStorageInfo.head.storageLevel}
              """.stripMargin)
    }
    /**/

    /** read the scorecard in
     */
    // val scorecardFile = "file:///home/christnp/Development/e6895/data/IPIP-NEO/input/IPIP-NEO-Scorecard.csv"
    val scorecardFile = "gs://eecs-e6895-bucket/input/IPIP-NEO-Scorecard.csv"
    logger.info(s"Reading in scorecard file '${scorecardFile}'.")
    var scorecardDF = readCSV(spark,scorecardFile).coalesce(1) // coalesce to 1 partition for broadcasting
    // scorecardDF.show()

   
      //////////////////// This works, but deprecated since I'm sending DF to ipipScores()
      // var scorecard = immutable.Map[Any,Any]()
      // var scorecardList = Seq[(String,String)]()
      // try
      // {
      //   scorecardList = scorecardDF.select($"ID",$"Key") // TODO: add $"Facet", etc.
      //     .rdd.map(r => (r(0),r(1))).collect.toList.asInstanceOf[Seq[(String, String)]]//.collectAsMap() //.collect
      // }
      // catch
      // {
      //   case unknown:  Throwable => logger.info(s"""Failed to load IPIP Scorecard, 
      //       |be sure format matches <ID>,<Key> at a minimum\n  at """ + unknown)
      //   // close the spark session
      //   spark.stop()
      //   throw new Exception(s">> Closed SparkSession [${spark.sparkContext.applicationId}");
      // }


     /** Load IPIP raw data in to Spark DF
     */
    logger.info(s"Reading in IPIP data file '${ipipFile}'.")
    var ipipDF = readCSV(spark,ipipFile)
        .repartition(numPartitions)
        .persist(StorageLevel.MEMORY_AND_DISK) //.cache // MEMORY_AND_DISK lessens burden on Memory, adds a little CPU overhead

    if(DEBUG > 1){
      logger.info(s"Initial ipipDF partitions: ${ipipDF.rdd.getNumPartitions}")
    }

    /** Load goal data into Spark DF
     */
    // Note:  this dataframe is small in size, let's broadcast it so a shuffle on
    //        the large DF is not needed.
    // ref: https://gist.github.com/dusenberrymw/30cebf98263fae206ea0ffd2cb155813
    // ref: https://community.cloudera.com/t5/Support-Questions/Does-Broadcast-variable-works-for-Dataframe/td-p/138426
    logger.info(s"Reading in goals file '${goalsFile}'.")
    val goalDF = readCSV(spark,goalsFile).coalesce(1) // coalesce to 1 partition for broadcasting
    /**/

// for spark-shell
// val ipipFile = "file:///home/christnp/Development/e6895/data/IPIP-NEO/final/IPIP120-final-1000.csv"
//  val scorecardFile = "file:///home/christnp/Development/e6895/data/IPIP-NEO/input/IPIP-NEO-Scorecard.csv"
//  var scorecardDF =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(scorecardFile)
//  var newDF =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ipipFile)
//  var scorecardList = scorecardDF.select($"ID",$"Key").rdd.map(r => (r(0),r(1))).collect.toList.asInstanceOf[Seq[(String, String)]]
//  val scorecardCols = scorecardList.map(f=>{col(f._1).as(f._2)})
//  val otherCols = scorecardList.map(idkey => idkey._1).toSet
//  val otherList = newDF.columns.filterNot(otherCols).map(f=>{col(f)})
//  val neList = otherList ++ scorecardCols
//  val newDF = newDF.select(neList:_*)
//  newDF.groupBy(scorecardCols.columns, axis=1).sum()
    // val factors = Map(
    //     "Openness" -> "OPN",
    //     "Conscientiousness" -> "CSN",
    //     "Extraversion" -> "EXT", 
    //     "Agreeableness" -> "AGR",
    //     "Neuroticism" -> "NEU"
    // )  
    // newDF.show(5)
    // var factorCols = Seq[String]()
    // factors.keys.foreach{ i =>  
    //     // now using a "scorecard" to generate the factor columns
    //     factorCols = scorecardDF.filter(col("Key").startsWith(factors(i))).select(col("ID")).rdd.map(r => r(0)).collect.toSeq.asInstanceOf[Seq[String]]
    //     // logger.info(s"For $i,\nfactorCols = $factorCols")
    //     newDF = newDF.withColumn(factors(i), factorCols.map(c => col(c)).reduce(_ + _)) // NC: changed from "i" to factors(i) (i.e., shortend name)
    //     newDF = newDF.drop(factorCols:_*)
    //   }
    // newDF.show(5)
    
    if (sc.getRDDStorageInfo.length > 0) 
    {
      logger.info(s"""RDD information before partition:
                |name: ${sc.getRDDStorageInfo.head.name}
                |numPartitions: ${sc.getRDDStorageInfo.head.numPartitions}
                |numCachedPartitions: ${sc.getRDDStorageInfo.head.numCachedPartitions}
                |memSize: ${sc.getRDDStorageInfo.head.memSize}
                |diskSize: ${sc.getRDDStorageInfo.head.diskSize}
                |diskSize: ${sc.getRDDStorageInfo.head.storageLevel}
              """.stripMargin)
    }
    /**/


    /** Do some data initialization/cleanup
     */
    // if the user defined ID column exists, change its name to "uuid"
    if(ipipDF.columns.contains(USER_ID_COLUMN)) 
    {
      ipipDF = ipipDF.withColumnRenamed(USER_ID_COLUMN,"uuid")
    }
    // else if the uuid column doesn't already exist then create it
    else if (!ipipDF.columns.contains("uuid")) 
    {
      ipipDF = ipipDF.withColumn("uuid", monotonically_increasing_id()).select("uuid", ipipDF.columns:_*)
    }
    // Filter out all IPCs that do not equal 1 (per  https://ipip.ori.org/, does not apply for IPIP-NEO data)
    if(ipipDF.columns.contains("IPC")) 
    {
      ipipDF = ipipDF.filter($"IPC" === 1)
    }

    if(DEBUG > 0)
    {
      if(_set_size > 0.0){
        val _fraction : Double = _set_size/ipipDF.count().toDouble
        ipipDF = ipipDF.orderBy("uuid")
          // .sample(false,_fraction,seed=314156) //(withReplacement, fraction, seed)
          .limit(_set_size.toInt)
          .repartition(numPartitions)
        logger.info(s"Rows in sampled DF: ${ipipDF.count()}")
      }
      logger.info(s"Original IPIP dataset")         
      logger.info(s"ipipDF partitions: ${ipipDF.rdd.getNumPartitions}")
      ipipDF.show(5)
      ipipDF.printSchema()
    }
    /**/


    /** Calculate the scores from the IPIP raw data  
     */
    // remove *_E and non-essential data using this blacklist
    // val blacklist = ipipDF.columns.toSeq.filter(_.endsWith("_E")) ++ Seq("screenw","screenh","introelapse","testelapse","endelapse")
    // TODO: is this the right way to broadcast?
    val score_start: Instant = Instant.now() //.now(fixedClock)
    logger.info(s"Calculating IPIP scores...")
    val ipipScoresDF = ipipScores(ipipDF,broadcast(scorecardDF)).repartition(numPartitions)//,blacklist=blacklist)//,raw=true)//,show=true)
    logger.info(s"IPIP scores calculated in ${Duration.between(score_start, Instant.now())}.")

    if(DEBUG > 1)
    {
      logger.info(s"Scored IPIP dataset")
      logger.info(s"ipipScoresDF partitions: ${ipipScoresDF.rdd.getNumPartitions}")
      ipipScoresDF.show(5)
      ipipScoresDF.printSchema()
    }
    /**/


    /** Emulate final dataset with goals assigned
     *  - literature backed algorithm for assigning goals (roughly)
     *    based on users personality
     *  - output will be a new dataset with UUIDs appropriately assigned 
     *    to the goal dataset
     */
    // TODO: is this the right way to broadcast?
    val final_start: Instant = Instant.now() //.now(fixedClock)
    logger.info(s"Creating final IPIP dataframe...")
    val finalDF = datasetEmu(ipipScoresDF,broadcast(goalDF)).repartition(numPartitions)//,show=true)
    logger.info(s"FinalDF finished in ${Duration.between(final_start, Instant.now())}.")
    // finalDF.printSchema()
    // finalDF.show(5)
    // finalDF.limit(10).printGoals(Seq("uuid","goals"))
    // finalDF.limit(5).printGoals(finalDF.columns.toSeq)
    // finalDF.limit(10).printGoals()

    if(DEBUG > 0)
    {
      logger.info(s"Final IPIP dataset")
      // logger.info(s"finalDF partitions: ${finalDF.rdd.getNumPartitions}")
      logger.info(s"finalDF partitions: ${finalDF.rdd.getNumPartitions}")
      finalDF.orderBy(asc("uuid")).show(20)
      logger.info(s"FinalDF size: ${finalDF.count()}")
      logger.info(s"FinalDF UUIDs: ${finalDF.groupBy("uuid").count()}")
      // df.agg(countDistinct("some_column"))

      // finalDF.printSchema()
    }


    /** Write final dataframe to Google Big Query table
     *  - be sure to identify depenendecy in Maven
     *  - be sure to add connector config to SparkSession
     *  - be sure to "mvn install" an uberJar
     *  - set "exit" to false so that the program does not exit
     *    on failure to store data in table
     */
    val GCP_BUCKET = "eecs-e6895-bucket"
    val GCP_PROJECT = "eecs-e6895-edu"
    val BQ_DATASET = "emu_dataset"
    var BQ_TABLE = "ipip120_"
    if(_set_size>0.0)  BQ_TABLE += _set_size.toInt.toString // toInt to get rid of decimal
    else BQ_TABLE += "final"
    

    try
    {
      val gbq_table = GCP_PROJECT+":"+BQ_DATASET+"."+BQ_TABLE
      logger.info(s"Storing data in Google BQ table '${gbq_table}'...")
      finalDF.orderBy(asc("uuid"))
        .writeToBQ(GCP_BUCKET,gbq_table,mode=SaveMode.Overwrite,exit=false) // exit gracefully
      logger.info(s"Data successfully stored in Google BQ!")
    }
    catch
    {
      case unknown:  Throwable => {
        logger.warn(s"Failed to save data to BQ table $BQ_TABLE,\n  at $unknown")
        logger.warn(s"Failing gracefully and continuing.")

      }
    }
    /**/

    //////////////////////////////// DEBUG /////////////////////////////////////
    if(DEBUG > 1) {
      var dbgDF = spark.emptyDataFrame

      // write the final dataframe; for csv, have to convert array to string
      dbgDF =  finalDF.orderBy(asc("uuid"))
        .withColumn("goals_str", array_join($"goals", "|"))
        .drop($"goals")
      dbgDF.show()
      // write it to a single (coalesce) CSV
      dbgDF.coalesce(1).write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save("../data/_debug/debug_final.csv")

      // get the goals count, convert goals to strings, and drop goals [Array]
      dbgDF = finalDF.withColumn("goals_str", array_join($"goals", "|"))
        .withColumn("goals_cnt",size($"goals")).orderBy(asc("uuid"))
        .select($"uuid",$"goals_cnt",$"goals_str").limit(100)
      dbgDF.show()
      // write it to a single (coalesce) CSV
      dbgDF.coalesce(1).write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save("../data/_debug/debug_goal_cnt.csv")

      // dbgDF.writeToBQ(GCP_BUCKET,GCP_PROJECT+":"+BQ_DATASET+".debug",mode=SaveMode.Overwrite)
    /**/
    val end: Instant = Instant.now() //.now(fixedClock)
    val tot_time = Duration.between(start, end)
    logger.info(s"Ending application at $end.")
    logger.warn(s"Total application time was $tot_time.")

    }
    // Method 2:
    // val j = Seq("uuid","lat_appx_lots_of_err") ++ factors.values.toSeq.map(k => k+"_Q") ++ Seq("goals")
    //   dbgDF.select(j.head,j.tail:_*).foreach { row => 
    //     println(row.toSeq)
    //   }
    // print("\n")
    // Method 3:
    // dbgDF.limit(10).printGoals(Seq("uuid","lat_appx_lots_of_err") ++ factors.values.toSeq.map(k => k+"_Q") ++ Seq("goals"))

    ////////////////////////////////////////////////////////////////////////////

    // close the SparkSession
    // logger.info(s"Closing SparkSession [" + spark.sparkContext.applicationId + "]")
    // spark.close()
  }


  /**
  * Implicit methods class that extend Spark Dataframe
  */
  // implicit class DatasetEmulatorExt(df : DataFrame) {  

  //   /** Prints the goals dataframe (but really, any dataframe)
  //   *
  //   *  @param cols list of column name strings to include in output
  //   *  @return prints each row as a list
  //   */
  //   def printGoals(cols: Seq[String]) = {
  //     df.select(cols.head,cols.tail:_*).foreach { row => 
  //       println(row.toSeq)
  //     }

  //   }
  //   /** Prints the goals dataframe (specifically)
  //   *
  //   *  @param none
  //   *  @return prints each row of DF, including only the uuid and goals
  //   */
  //   def printGoals() = {
  //   // df.foreach { row => row.toSeq.foreach{goal => println(goal) }
  //     val cols = Seq("uuid","goals")
  //     val goals = df.select(cols.head,cols.tail:_*).collect.toList
  //     // print results
  //     goals.foreach { tup=> {
  //       println(tup(0) + ": Goals")
  //       print(" >> " + tup(1))
  //       logger.info(s"\n")
  //       }
  //     }
  //   }
  // /** Writes data to Google BQ table
  //   *
  //   *  @param bucket string name of the  GCP bucket for temp storage
  //   *  @param table string name of the GCP BQ dataset.table to store the data
  //   *  @return prints each row of DF, including only the uuid and goals
  //   */
  //   def writeToBQ(bucket: String,table: String = "temp",mode: SaveMode = SaveMode.Append) = {
  //     // get the SparkSession from the DataFrame
  //     // Use the Cloud Storage bucket for temporary BigQuery export data used
  //     // by the connector.
  //     // spark.conf.set("temporaryGcsBucket", bucket)
  //     // Saving the data to BigQuery.
  //     try{
  //       df.write.format("bigquery")
  //         .option("table",table)
  //         .option("temporaryGcsBucket",bucket)
  //         .mode(mode)
  //         // .option("credentials", gcp_cred)
  //         .save()
  //     } catch {
  //       case unknown:  Throwable => logger.info(s"Failed to save data to BQ table " + table + " ','\n  at " + unknown)
  //       val spark =  df.sparkSession
  //       logger.info(s"\n>> Closing SparkSession [" + spark.sparkContext.applicationId + "]\n")
  //       spark.stop()

  //       throw new Exception("Not sure what to do... ");

  //     }
  //   }

  // /** Reads data from Google BQ table
  //   *
  //   *  @param bucket string name of the  GCP bucket for temp storage
  //   *  @param table string name of the GCP BQ dataset.table to read data from
  //   *  @return prints each row of DF, including only the uuid and goals
  //   */
  //   def readFromBQ(bucket: String,table: String = "temp") = {
  //     // get the SparkSession from the DataFrame
  //     // Use the Cloud Storage bucket for temporary BigQuery export data used
  //     // by the connector.
  //     // spark.conf.set("temporaryGcsBucket", bucket)
  //     // Saving the data to BigQuery.
  //     try{
  //       df.read.format("bigquery")
  //         .option("table",table)
  //         .load()
  //     } catch {
  //       case unknown:  Throwable => logger.info(s"Failed to load data from BQ table " + table + " ','\n  at " + unknown)
  //       val spark =  df.sparkSession
  //       logger.info(s"\n>> Closing SparkSession [" + spark.sparkContext.applicationId + "]\n")
  //       spark.stop()

  //       throw new Exception("Not sure what to do... ");

  //     }
  //   }
  // }
}