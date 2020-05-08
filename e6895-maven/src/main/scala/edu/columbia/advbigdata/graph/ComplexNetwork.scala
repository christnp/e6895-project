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

// spark-mllib_2.11 artifact (for RDD matrix operations)
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._ //{RowMatrix,IndexedRow,IndexedRowMatrix,BlockMatrix,MatrixEntry}

// spark-bigquery_2.11 artifacts
import com.google.cloud.spark.bigquery._
// import com.google.auth.oauth2.GoogleCredentials

// spark-graphx_2.11 artifacts
import org.apache.spark.graphx._

// graphframes_2.11 artifact
import org.graphframes._

// neo4j-spark-connector artifact
import org.neo4j.spark._
import org.neo4j.spark.dataframe._

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
import java.{util => ju}


// Test data: file:///home/christnp/Development/e6895/data/IPIP-FFM-data-8Nov2018/data-small.csv
/**
 * Factory for [[graph.ComplexNetwork]] 
 * 
 */
object ComplexNetwork {
  
  def logger : Logger = LoggerFactory.getLogger( ComplexNetwork.getClass )

  private val prgName = "ComplexNetwork"
  
  // define the neo4j parameters
  private var neo4j_ecrypt = "false"
  private var neo4j_user = "neo4j"
  private var neo4j_pass = "password"
  private var neo4j_host = "34.82.120.135"
  private var neo4j_port = "7687" //bolt port
  private var neo4j_url = s"bolt://${neo4j_host}:${neo4j_port}" //"neo4j://localhost"

  // Create the Spark Session (access with this.sparkSession)
  private def sparkSession: SparkSession = SparkSession.builder()
      .config("spark.neo4j.bolt.encryption", neo4j_ecrypt)
      .config("spark.neo4j.bolt.user", neo4j_user)
      .config("spark.neo4j.bolt.password", neo4j_pass)
      .config("spark.neo4j.bolt.url", neo4j_url)
      // .master("local[*]") //"Spark://master:7077"
      .appName(prgName)
      .getOrCreate()

  // Google BQ variables
  val GCP_BUCKET = "eecs-e6895-bucket"
  val GCP_PROJECT = "eecs-e6895-edu"
  val BQ_DATASET = "emu_dataset"
  // val BQ_TABLE = "emudata_2"
  val BQ_TABLE = "ipip120_10000"

  // Output format
  val SAVE_CSV = true // set true to store graphframe as CSV

  /**
   * Function to ingest a DataFrame with layer column name identified
   * and return a GraphFrame object. The idea is that multiple layers
   * can be generated using the same process because each layer utilizes
   * similar data (i.e., name, uuid, location, etc.). 
   *
   *  @param df the source DataFrame
   *  @param layerCol string column name representing the layer data
   *  @param partitions integer defines the number of partitions to use 
   *  @param csv boolean flag when true saves graph DF as CSV on Google Storage
   *  @param persist boolean flag when true persists the graph DF to memory
   *  @param verbose boolean flag when true sets verbose logging for this function 
   *  @return a GraphFrame object
   */
  def buildMono(
      df: DataFrame, 
      layerCol: String, 
      partitions: Int = 0,
      csv: Boolean = false,
      persist: Boolean = true,
      verbose: Boolean = false
    ) : GraphFrame = {
    
    logger.info(s"Starting buildMono() for '${layerCol}'...")
    /** get the SparkSession, SparkContext, and import spark implicits
     */
    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    /** define profiles for executive, manager, and workers */
    // val excs = Map(
    //     "OPN" -> (0.0,1.0),
    //     "CSN" -> (-3.0,-2.0),
    //     "EXT" -> (2.0,3.0),
    //     "AGR" -> (1.0,2.0),
    //     "NEU" -> (-3.0,-2.0)
    // )
    // val mgrs = Map(
    //     "OPN" -> (0.0,1.0),
    //     "CSN" -> (-3.0,-2.0),
    //     "EXT" -> (2.0,3.0),
    //     "AGR" -> (1.0,2.0),
    //     "NEU" -> (-3.0,-2.0)
    // )
    // val wkrs = Map(
    //     "OPN" -> (0.0,1.0),
    //     "CSN" -> (-3.0,-2.0),
    //     "EXT" -> (2.0,3.0),
    //     "AGR" -> (1.0,2.0),
    //     "NEU" -> (-3.0,-2.0)
    // )
    /** define the columns */
    // var colComm = Seq("uuid","name","date","age","sex","country","goals") // for now, include goals as attributes
    var colComm = Seq("uuid","name","date","age","sex","country") // for now, remove goals
    colComm ++= Seq(layerCol)
    // (1) establish the vertex DF from the master DF
    var vertDF = df.select(colComm.head, colComm.tail: _*)
    // repartition if partition is defined
    if(partitions>0) vertDF = vertDF.repartition(partitions)
    // force persist
    vertDF.persist(StorageLevel.MEMORY_AND_DISK).count() // MEMORY_AND_DISK lessens burden on Memory, adds a little CPU overhead

    // rename UUID as ID for GraphFrames, else create one if UUID D.N.E.
    try
    {
        vertDF = vertDF.withColumnRenamed("uuid", "id") 
    }
    catch
    {
        case unknown:  Throwable => {
            logger.warn(s"Failed rename col('id') as col('id') \n  at $unknown")
            logger.info(s"Creating col('id') with increasing_id()")
            vertDF = vertDF.withColumn("id", monotonically_increasing_id())
                .select("id", vertDF.columns:_*)
        }
    }

    // statistics on the data
    val layerMean = vertDF.select(mean(layerCol)).collect.head.getDouble(0)
    val opnStddev = vertDF.select(stddev(layerCol)).collect.head.getDouble(0)

    if(verbose)
    {
      logger.info(s"${layerCol}-Mean = ${layerMean.toString()}")  
      logger.info(s"${layerCol}-Stddev = ${opnStddev.toString()}")
    }

    // (2) create two temporary DFs for edges ("src","LAYER_src","dst","LAYER_dst")
    //     avoid column name collisions by renaming columns
    val colFeat_e = Seq(layerCol+"_src",layerCol+"_dst")
    var tempDF_src = vertDF.select(col("id").as("src"),col(layerCol).as(colFeat_e(0))) 
    var tempDF_dst = vertDF.select(col("id").as("dst"),col(layerCol).as((colFeat_e(1))))
 
    // (3) create the edge dataframe of [all connections,connections > opnThresh]
    // Z-score values tells us how far above (+) or below (-) the mean population
    // score a user is; therefore, if we want users mean scores that are at or 
    // above 1-sigma, then our threshold for the mean values would be 1
    /** make connections
     *  - fitness function is a standard mean personality between nodes
     *  - only connect nodes if factor fitness matches criteria
     *  - only connect src to dst IFF src.zscore < dst.zscore (DAG)
     *  - (TBD) only connect src to dst IFF src.zscore != dst.zscore 
     */ 
    // just to make the fitness function logic a little cleaner, define logic
    val fitSrcDst = (tempDF_src(colFeat_e(0))+tempDF_dst(colFeat_e(1)))/2.0       // src dst fitness function
    val eqSrcDst = (tempDF_src(colFeat_e(0)) !== tempDF_dst(colFeat_e(1)))        // src not equal dst
    val ltSrcDst = (tempDF_src(colFeat_e(0)) < tempDF_dst(colFeat_e(1)))          // src less than dst
    // Default:
    var joinCond = ((tempDF_src("src") !== tempDF_dst("dst")) && 
                    (fitSrcDst >= 2.0)) 
    // define the join condition for each of the personality factors
    if(layerCol.startsWith("OPN"))
    {
        joinCond = ((tempDF_src("src") !== tempDF_dst("dst")) &&                            // don't allow loopback, nodes linked to self 
                    (fitSrcDst >= 1.0 && fitSrcDst < 3.0) &&                        // >= 3.0? This is upper 97%
                    (tempDF_src(colFeat_e(0)) !== tempDF_dst(colFeat_e(1))) &&
                    (tempDF_src(colFeat_e(0)) < tempDF_dst(colFeat_e(1))))
    }
    else if(layerCol.startsWith("CSN"))
    {
        joinCond = ((tempDF_src("src") !== tempDF_dst("dst")) &&                            // don't allow loopback, nodes linked to self 
                    ((fitSrcDst >= 1.0 && fitSrcDst < 3.0) ||                       // >= 3.0? This is upper 97%
                     (fitSrcDst > -3.0 && fitSrcDst <= -1.0) ) &&
                    (tempDF_src(colFeat_e(0)) !== tempDF_dst(colFeat_e(1))) &&
                    (tempDF_src(colFeat_e(0)) < tempDF_dst(colFeat_e(1))))
    }
    else if(layerCol.startsWith("EXT"))
    {
        joinCond = ((tempDF_src("src") !== tempDF_dst("dst")) &&                            // don't allow loopback, nodes linked to self 
                    (fitSrcDst >= 1.0 && fitSrcDst < 3.0) &&                      // >= 3.0? This is upper 97%
                    (tempDF_src(colFeat_e(0)) !== tempDF_dst(colFeat_e(1))) &&
                    (tempDF_src(colFeat_e(0)) < tempDF_dst(colFeat_e(1))))
    }
    else if(layerCol.startsWith("AGR"))
    {
        joinCond = ((tempDF_src("src") !== tempDF_dst("dst")) &&                            // don't allow loopback, nodes linked to self 
                    ((fitSrcDst >= 1.0 && fitSrcDst < 3.0) ||                       // upper AGR connect to upper EXT
                     (fitSrcDst > -1.0 && fitSrcDst < 1.0) ) &&                     // average AGR joes connect to low NEU joes
                    (tempDF_src(colFeat_e(0)) !== tempDF_dst(colFeat_e(1))) &&
                    (tempDF_src(colFeat_e(0)) < tempDF_dst(colFeat_e(1))))
    }
    else if(layerCol.startsWith("NEU"))
    {
        joinCond = ((tempDF_src("src") !== tempDF_dst("dst")) &&                            // don't allow loopback, nodes linked to self 
                    (fitSrcDst >= -3.0 && fitSrcDst < -1.0) &&                      // low NEU = high EST
                    (tempDF_src(colFeat_e(0)) !== tempDF_dst(colFeat_e(1))) &&
                    (tempDF_src(colFeat_e(0)) > tempDF_dst(colFeat_e(1))))          // swapped because we want low values, not high!
    }
    else
    {
        logger.warn(s"The layerCol name '${layerCol}' does not exist, using default join condition.")
    }

    // perform the join and repartition if defined
    var edgeDF = tempDF_src.join(tempDF_dst, joinCond)
    if(partitions>0) edgeDF = edgeDF.repartition(partitions)

    // force persist
    edgeDF.persist(StorageLevel.MEMORY_AND_DISK).count() // MEMORY_AND_DISK lessens burden on Memory, adds a little CPU overhead

    // remove the temps from memory
    // TODO: do we really want to do this?
    tempDF_src.unpersist().count()
    tempDF_dst.unpersist().count()

    edgeDF = edgeDF.withColumn(layerCol+"_fit",
      ((edgeDF(colFeat_e(0))+edgeDF(colFeat_e(1)))/2.0)).drop(colFeat_e:_*)

    // log some information
    if(verbose)
    {
      logger.info(s"edgeDF partitions = ${edgeDF.rdd.getNumPartitions}")
      logger.info(s"vertDF partitions = ${vertDF.rdd.getNumPartitions}")
      logger.info(s"Vertex DataFrame...")
      vertDF.show(5)   
      logger.info(s"Edge DataFrame...")
      edgeDF.show(5) 
      logger.info(s"Finished exporting layer information for '${layerCol}'")
    }

    // (4) create, store (optional), persist (optional), and return the Graph DF
    val gdf = GraphFrame(vertDF, edgeDF)
    if(persist) gdf.persist(StorageLevel.MEMORY_AND_DISK) 

    if(csv) {
      // TODO: is a global GCP path okay?
      val gcp_path = s"gs://${GCP_BUCKET}/output"
      logger.info(s"Saving data as CSV to '$gcp_path'")
      // save vertices/nodes
      var file_path = s"${gcp_path}/${layerCol}_raw_vert.csv"
      gdf.vertices.writeToCSV(file_path)
      logger.info(s"Successfully saved nodes as '$file_path'")
      // save edges
      file_path = s"${gcp_path}/${layerCol}_raw_edges.csv"
      gdf.edges.writeToCSV(file_path)
      logger.info(s"Successfully saved edges as '$file_path'")
    }
    
    return gdf
  }

  /**
    * 
    *
    * @param ogdf
    * @param cgdf
    * @param egdf
    * @param agdf
    * @param ngdf
    * @param partitions
    * @param csv
    * @param persist
    * @param verbose
    */

   /**
    * Function to ingest a DataFrame with layer column name identified
    * and return a GraphFrame object. The idea is that multiple layers
    * can be generated using the same process because each layer utilizes
    * similar data (i.e., name, uuid, location, etc.). 
    *
    *  @param ogdf 
    *  @param cgdf
    *  @param egdf
    *  @param agdf
    *  @param ngdf
    *  @param partitions
    *  @param csv
    *  @param persist
    *  @param verbose
    *  @param df the source DataFrame
    *  @param layerCol string column name representing the layer data
    *  @param partitions integer defines the number of partitions to use 
    *  @param csv boolean flag when true saves graph DF as CSV on Google Storage
    *  @param persist boolean flag when true persists the graph DF to memory
    *  @param verbose boolean flag when true sets verbose logging for this function 
    *  @return a GraphFrame object
    */
  def buildMulti(
      // df_list: List[DataFrame], 
      gdfs: Seq[(String,GraphFrame)],
      // ogdf: GraphFrame, 
      // cgdf: GraphFrame, 
      // egdf: GraphFrame, 
      // agdf: GraphFrame, 
      // ngdf: GraphFrame, 
      partitions: Int = 0,
      csv: Boolean = false,
      persist: Boolean = true,
      verbose: Boolean = false
    ) : GraphFrame = {
    
    val start: Instant = Instant.now() //.now(fixedClock)
    // val start: Int = 100
    logger.info(s"Starting buildMulti() at $start...")
    /** get the SparkSession, SparkContext, and import spark implicits
     */
    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._
    
    /** build adjacency matrix */
    // var fullRDD = RDD()    
    // var modeOneRDD: RDD[MatrixEntry] = RDD[MatrixEntry]()
    // var modeOneRDD = spark.sparkContext.emptyRDD[MatrixEntry] // moved to val _ = sc.union() below
    var mode1Seq = Seq[RDD[MatrixEntry]]()
    var mode2Seq = Seq[RDD[MatrixEntry]]()
    var mode3Seq = Seq[RDD[MatrixEntry]]()
    gdfs.zipWithIndex.foreach{ case(layer_gdf,layer_num) =>

      val layer = layer_gdf._1
      val ogdf = layer_gdf._2

      logger.info(s"Adding '${layer}' layer...")      
      val V_o = ogdf.vertices
      val E_o = ogdf.edges
      
      // logger.info(s"Rows in V_o: ${V_o.count()}")
      // logger.info(s"V_o =")
      // logger.info(s"V_o Class = ${V_o.getClass}")
      // V_o.orderBy(asc("id")).show()
      

      // logger.info(s"Rows in E_o: ${E_o.count()}")
      // logger.info(s"E_o =")
      // logger.info(s"E_o Class = ${E_o.getClass}")
      // E_o.orderBy(asc("src")).show()

      // var pairs = E_o.select("src","dst","OPN_Z_fit") //"weight" is from PageRank
      // // val p_cols = pairs.columns
      // // pairs = pairs.groupBy("src").agg(collect_list(struct(p_cols.head, p_cols.tail: _*)).as("adj")).collect//.agg(collect_list("dst")).collect//.collect.map(row => Seq(row._1.getInt(0),row._2.getInt(0)))
      // // pairs = pairs.groupBy("src").agg(collect_list(struct(p_cols.head, p_cols.tail: _*)).as("adj")).select("adj")//.agg(collect_list("dst")).collect//.collect.map(row => Seq(row._1.getInt(0),row._2.getInt(0)))
      // pairs = pairs.groupBy("src").agg(collect_list(struct("dst","OPN_Z_fit")).as("adj"))//.select("adj")//.agg(collect_list("dst")).collect//.collect.map(row => Seq(row._1.getInt(0),row._2.getInt(0)))
      // pairs.collect.foreach(println)
      // logger.info(s"pairs Class = ${pairs.getClass}")
      // logger.info(s"pairs =")
      
      /** the edge DataFrame is the adjacency matrix with the vertex 
       *  DataFrame used to fill in non-connected nodes
      */
      // val adjWeight = avg("OPN_Z_fit")
      val adjWeight = lit(1.0)
      // pivot the edge DataFrame to get our DataFrame in adj. matrix format
      var adjDF = E_o.select("src","dst",s"${layer}_Z_fit")
        .orderBy(asc("src"))
        .groupBy("src")
        .pivot("dst")
        .agg(adjWeight) // 
        .drop(s"${layer}_Z_fit")
        .withColumnRenamed("src","id")
      // these are the final columns for the adjacency DataFrame
      val adjCols = V_o.select("id").orderBy(asc("id")).rdd.map(_(0).toString).collect.toList
      val adjCols_id =  List("id") ++ adjCols // need "id" column for DF operations
      // not all nodes have links; add missing columns and set to zero
      // NOTE: this works because we alias edge.src as edge.id
      adjCols.foreach{ c =>
        if(!adjDF.columns.contains(c)) adjDF = adjDF.withColumn(c,lit(0.0))
      }

      // similarly, we must add missing rows; we use a join with the vertex DF
      // NOTE: when rows are large, this might be very inefficient 
      adjDF = V_o.select("id").as("tmp")
        .join(adjDF.as("adj"), col("tmp.id") === col("adj.id"), "outer")
        .select(col("tmp.id"),col("adj.*"))
        .drop(col("adj.id"))
      
      // pretty up the output (i.e., order rows and columns)
      adjDF = adjDF.select(adjCols_id.head,adjCols_id.tail:_*)
        .orderBy(asc("id"))
        .na.fill(0,adjCols)

      println("New Joined DF")
      adjDF.show()
      // adjDF.printSchema()

      // get Adjacency matrix columns
      val matCols = adjCols.map(col(_))

      // set matrix dimension values
      val N = adjDF.count.toLong
      val L = layer_num
      
      /** now we will convert the DataFrame to an RDD based adjacency matrix 
       *  NOTE: we choose
      */
   
      /////////////////////////////////////////////////////////////////////////
      // // val tmpRDD = adjDF.as("arr").as[Array[Int]].rdd
      // //   .zipWithIndex()
      // //   .map{ case(arr,index) => IndexedRow(index,Vectors.dense(arr.map(_.toDouble)))}

      // // val adjRDD = new IndexedRowMatrix(tmpRDD)

    
      
      // // example
      // // mat = IndexedRowMatrix(mydataframe.map(row => new IndexedRow(row[0], Vectors.dense(row[1:]))))
      // // val tmpRDD = adjDF.rdd//.as[Array[Int]].rdd
      // //   .map(row => (row(0),row.toSeq.tail.toList)) // create an ("id",*links) tuple
      // //   // .groupByKey()
      // //   .map{ case(id,links) => 
      // //     // val width = links.length
      // //     // var link_z = Seq[(Int,Double)]()
      // //     // links.zipWithIndex.map{case (elm,idx) => link_z=link_z:+((idx,elm.toDouble))}
      // //     println(s"$id,$links")
      // //     doubleArray = Array(r.getInt(5).toDouble, r.getInt(6).toDouble) 
      // //     Vectors.dense(doubleArray) 
      // //     // new IndexedRow(id, Vectors.dense(links.flatMap(_.toDouble)))
      // //     // new IndexedRow(id, Vectors.sparse(width,link_z))
      // //   }
        
      // // ref 1: https://stackoverflow.com/questions/51184319/convert-dataframe-into-spark-mllib-matrix-in-scala
      // // ref 2: https://lamastex.github.io/scalable-data-science/sds/1/6/db/xtraResources/ProgGuides1_6/MLlibProgrammingGuide/dataTypes/006_IndexedRowMatrix/
      // // TODO: use my own indices to align with userID
      // val tmpRDD = adjDF.select(array(matCols:_*).as("arr")).as[Array[Double]].rdd
      // // val tmpRDD = adjDF.select(adjCols.head,adjCols.tail:_*)
      // // tmpRDD.as("arr").as[Array.ofDim[Double](tmpRDD.columns.length)].rdd
      // .zipWithIndex()
      // // .map{ case(arr,index) => IndexedRow(index, Vectors.dense(arr.map(_.toDouble)))}
      // .map{ case(arr,index) => IndexedRow(index, Vectors.dense(arr.map(_.toDouble)))}

      // val rowMat = new IndexedRowMatrix(tmpRDD)
      
      // val corMat = rowMat.toCoordinateMatrix()
      // val corMat_T = corMat.transpose

      // val tmpRDD2 = adjDF.select(array(matCols:_*).as("arr")).as[Array[Double]].rdd
      // .zipWithIndex()
      // .map{ case(arr,index) => IndexedRow(index, Vectors.dense(arr.map(_.toDouble+10)))}
      // // val rdd = sc.union([tmpRDD, tmpRDD2])

      // val uMat = new IndexedRowMatrix(tmpRDD ++ tmpRDD2)
      // // print(s"uMat.collect()\n")
      // // uMat.rows.collect.foreach(println)
      // val uCorMat = uMat.toCoordinateMatrix()
      // val uCorMat_T = uCorMat.transpose
      // print(s"\nuCorMat.collect()\n")
      // uCorMat.entries.collect.foreach(println)
      /////////////////////////////////////////////////////////////////////////



      // print(s"uCorMat.groupbByKey.collect()\n")
      // uCorMat.entries.groupByKey.collect.foreach(println)
      // print(s"uCorMat_T.collect()\n")
      // uCorMat_T.entries.collect.foreach(println)

      // uCorMat.entries.map{entry => println(entry)}
      // entries.

      // adjacency matrix as RDD (each mone-n matrix generated in parallel)
      val adjRDD = adjDF.select(array(matCols:_*).as("arr")).as[Array[Double]].rdd
      /**
        * Mode-1 Matricization
        */
      val tmpOneRDD = adjRDD.zipWithIndex
        .flatMap{ case(arr,i) => arr.zipWithIndex.map{case(v,j) => MatrixEntry(i, L*N+j, v.toDouble)}}
      mode1Seq = mode1Seq ++ Seq(tmpOneRDD)
       /**
        * Mode-2 Matricization
        *  - Mode-2 is just Mode-1 Matrices transposed... swap just i and j
        */
       val tmpTwoRDD = adjRDD.zipWithIndex
        .flatMap{ case(arr,i) => arr.zipWithIndex.map{case(v,j) => MatrixEntry(j, L*N+i, v.toDouble)}}
      mode2Seq = mode2Seq ++ Seq(tmpTwoRDD)
       /**
        * Mode-3 Matricization
        *  - Mode-3 is is a bit trickier... rows are fibers ("L") and columns 
        *    represent each element in the fiber (i,j)
        */
       val tmpThreeRDD = adjRDD.zipWithIndex
        .flatMap{ case(arr,i) => arr.zipWithIndex.map{case(v,j) => MatrixEntry(L, i*N+j, v.toDouble)}}
      mode3Seq = mode3Seq ++ Seq(tmpThreeRDD)
      
      // val clock: Instant = Instant.now() 
      // modeOneRDD = modeOneRDD.union(tmpOneRDD) // moved to sc.union([all]) below
      // to combine by row instead use rddPart1.unionAll(rddPart2)
      // println(s"Individual Union took: ${Duration.between(clock,Instant.now()).toMillis()} ms")

      // print(s"\nmodeOneRDD.collect()\n")
      // modeOneRDD.collect.foreach(x => println(x.getClass))
      // modeOneRDD.collect.foreach(println)
      // print(s"\n")
      
      // val entries: RDD[MatrixEntry] = sc.parallelize(Array(MatrixEntry(0,1,1),MatrixEntry(1,0,2),MatrixEntry(2,1,3)))
      // print(s"\nentries.collect()\n")
      // entries.collect.foreach(println)
      // print(s"\n")

      // val modeOne = uCorMat.entries
        // .map{case MatrixEntry(i, j, v) => ((i, L*N+j), v)}
      // val modeOne = new CoordinateMatrix(modeOneRDD)
      // print(s"\nmodeOne.entires.collect()\n")
      // modeOne.entries.collect.foreach(println)
      // print(s"\n")

      // val entriesCord = new CoordinateMatrix(entries)
      // print(s"\nentriesCord.entires.collect()\n")
      // entriesCord.entries.collect.foreach(println)
      // print(s"\n")

      // flatten coordinate matrix into a single row
      // uCorMat.entries.map{ case(arr,index) => IndexedRow(index, Vectors.dense(arr.map(_.toDouble)))}

        /** now we will apply the CP-ALS algorithm
      * 
      */
      
      // println("\nRDD contents:")
      // tmpRDD.collect.foreach(println)
      /**
        * Convert to block matrix
        */
      // val m = rowMat.numRows()
      // val n = rowMat.numCols()
      // println("\nrowMat contents:")
      // println(s"numRows = $m,\nnumCols = $n")
      // rowMat.rows.collect.foreach(println)
      // if(m != n)
      // {
      //   throw new IllegalArgumentException(
      //     s"Adjacency matrix dimensions must agree. Rows: $m, Cols: $n")
      // }
      // println("\n")

      // val blkMat: BlockMatrix = rowMat.toBlockMatrix(1,5).cache() // m = rows, n = cols must be int
      // val blkMat_2: BlockMatrix = rowMat.toBlockMatrix(1,5).cache() // m = rows, n = cols must be int

      // // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
      // // Nothing happens if it is valid.
      // blkMat.validate()

      // // Calculate A^T A.
      // val ata = blkMat.transpose.multiply(blkMat)
      // val apa = blkMat.add(blkMat_2)

      // val x = blkMat.numRows()
      // val y = blkMat.numCols()
    
      // println("\nblkMat contents:")
      // println(s"numRows = $x,\nnumCols = $y")
      // blkMat.blocks.collect.foreach(println)
      // println("\nblkMat.transpose.multiply(blkMat):")
      // ata.blocks.collect.foreach(println)
      // println("\nblkMat.add(blkMat_2)")
      // apa.blocks.collect.foreach(println)
      // // blkMat.rows.collect.foreach(println)
    
    
    } // end foreach GraphFrame

    var unionStart: Instant = Instant.now() 
    // modeOneRDD = sc.union(mode1Seq.head,mode1Seq.tail:_*)
    val mode1Rdd = sc.union(mode1Seq)
    println(s"Mode-1 Union took: ${Duration.between(unionStart,Instant.now()).toMillis()} ms")
    // print(s"\nmode1Rdd.collect()\n")
    // // modeOneRDD.collect.foreach(x => println(x.getClass))
    // mode1Rdd.collect.foreach(println)
    // print(s"\n")

    val mode1Mat = new CoordinateMatrix(mode1Rdd)
    print(s"mode1Mat: numRows=${mode1Mat.numRows}, numCols=${mode1Mat.numCols}\n")
    print(s"mode1Mat as IndexedRowMatrix\n")
    mode1Mat.toIndexedRowMatrix.rows.collect.foreach(println)
    print(s"\n")

    unionStart = Instant.now() 
    val mode2Rdd = sc.union(mode2Seq)
    println(s"Mode-2 Union took: ${Duration.between(unionStart,Instant.now()).toMillis()} ms")
    // print(s"\nmode2Rdd.collect()\n")
    // // modeOneRDD.collect.foreach(x => println(x.getClass))
    // mode2Rdd.collect.foreach(println)
    // print(s"\n")

    val mode2Mat = new CoordinateMatrix(mode2Rdd)
    print(s"mode2Mat: numRows=${mode2Mat.numRows}, numCols=${mode2Mat.numCols}\n")
    print(s"mode2Mat as IndexedRowMatrix\n")
    mode2Mat.toIndexedRowMatrix.rows.collect.foreach(println)
    print(s"\n")

    unionStart = Instant.now() 
    val mode3Rdd = sc.union(mode3Seq)
    println(s"Mode-3 Union took: ${Duration.between(unionStart,Instant.now()).toMillis()} ms")
    // print(s"\nmode3Rdd.collect()\n")
    // // modeOneRDD.collect.foreach(x => println(x.getClass))
    // mode3Rdd.collect.foreach(println)
    // print(s"\n")

    val mode3Mat = new CoordinateMatrix(mode3Rdd)
    print(s"mode3Mat: numRows=${mode3Mat.numRows}, numCols=${mode3Mat.numCols}\n")
    print(s"mode3Mat as IndexedRowMatrix\n")
    mode3Mat.toIndexedRowMatrix.rows.collect.foreach(println)
    print(s"\n")

    // You can do this by accessing the underlying blocks RDD. E.g. blockMatrix.blocks.filter{case ((x, y), matrix) => x == i && y == j} will give you an RDD[((Int, Int), Matrix)]
    // println(s"\nadjRDD type = ${adjRDD.getClass}")
    // println(s"adjRDD type = ${adjRDD.foreach(println)}\n")

    // val adjMat = adjDF.select()
    
    
    // val newCols = Seq("id") ++ adjDF.columns.filterNot(_ == "id").sorted
    // val newCols = adjDF.columns.sortWith(_ < _)
    // println(s"newCols = $newCols")

    // adjDF = adjDF.select(newCols.head,newCols.tail:_*)

    // var adjDF = tmp.join(edges, adjCols,"outer")
    // var adjDF = edges.join(tmp, joinCond,"leftsemi")
   

    // V_o.select("id").rdd.map(r => r(0)).collect.foreach{ node=> //toSeq.asInstanceOf[Seq[String]]
    //   println(s"\nnode = ${node}")
    //   println(s"node Class = ${node.getClass}")

    //   pairs.collect.foreach{pair =>
    //     println(s"\npair Class = ${pair.getClass}")
    //     println(s"node=$node, pair=$pair\n")
    //   }
    // }
    // println("\n")

    // pairs.foreach(println)

    // logger.info(s"Running connected components...")
    // val tmp = ogdf.connectedComponents.run()
    // tmp.show()
    // val V_a = agdf.vertices.select("id").collect.map(row => row.getLong(0))
    // val E_a = agdf.edges.select("src","dst").groupBy("src").agg(collect_list("dst")).collect//.collect.map(row => Seq(row._1.getInt(0),row._2.getInt(0)))
    // agdf.vertices.show()
    // agdf.edges.show()
    // logger.info(s"E_a =")
    // logger.info(s"${E_a.foreach(println)}")
    // logger.info(s"E_a Class = ${E_a .getClass}")

    // var arr = Array(Array("a", "b", "c"))
    //arr ++= Array(Array("d", "e"))


    return gdfs.head._2
  }

  /**
   * Function to ingest a Graph DataFrame (GraphFrame) and analyze the intralayer
   * based on the algorithm (alg) defined. Optionally store results in a Google
   * Storage bucket (defined globally)
   *
   *  @param gdf the source DataFrame
   *  @param layerCol string column name representing the layer (used for CSV)
   *  @param alg string defines algorithm to be used for analysis
   *  @param csv boolean flag when true saves graph DF as CSV on Google Storage
   *  @param verbose boolean flag when true sets verbose logging for this function 
   *  @return a GraphFrame object
   */
  def intraLayerAnlaysis(
      gdf: GraphFrame, 
      layerCol: String, 
      alg: String,
      csv: Boolean = false,
      verbose: Boolean = false
    ) : GraphFrame = {
    
      logger.info(s"Starting intraLayerAnlaysis() now...")
    
    /** get the SparkSession, SparkContext, and import spark implicits
     */
    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    var tmp_gdf = gdf
    if(alg.toLowerCase() == "pagerank")
    {
      // for individual Run PageRank until convergence to tolerance "tol".
      tmp_gdf = tmp_gdf.pageRank.resetProbability(0.15).tol(0.01).run()
    }
    else
    {
      logger.warn(s"Unsupported algorithm '$alg', returning original GraphFrame")
      return tmp_gdf
    }

    if(csv) 
    {
      // TODO: is a global GCP path okay?
      val gcp_path = s"gs://${GCP_BUCKET}/output"
      logger.info(s"Saving data as CSV to '$gcp_path'")
      // save vertices/nodes
      var file_path = s"${gcp_path}/${layerCol}_PageRank_vert.csv"
      tmp_gdf.vertices.writeToCSV(file_path)
      logger.info(s"Successfully saved nodes as '$file_path'")
      // save edges
      file_path = s"${gcp_path}/${layerCol}_PageRank_edges.csv"
      tmp_gdf.edges.writeToCSV(file_path)
      logger.info(s"Successfully saved edges as '$file_path'")
    }
    
    // if(persist) tmp_gdf.persist(StorageLevel.MEMORY_AND_DISK) 

    return tmp_gdf
  }

  /**
   * Function to ingest a Graph DataFrame (GraphFrame) and analyze the interlayer
   * network. Optionally store results in a Google Storage bucket (defined globally)
   *
   *  @param gdf the source DataFrame
   *  @param layerCol string column name representing the layer (used for CSV)
   *  @param alg string defines algorithm to be used for analysis
   *  @param csv boolean flag when true saves graph DF as CSV on Google Storage
   *  @param verbose boolean flag when true sets verbose logging for this function 
   *  @return a GraphFrame object
   */
  def interLayerAnlaysis(
      gdf: GraphFrame, 
      layerCol: String, 
      alg: String,
      csv: Boolean = false,
      verbose: Boolean = false
    ) : GraphFrame = {

    logger.info("interLayerAnlaysis not yet developed.")
    /** get the SparkSession, SparkContext, and import spark implicits
     */
    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    var tmp_gdf = gdf

    // if(persist) tmp_gdf.persist(StorageLevel.MEMORY_AND_DISK) 
    return tmp_gdf
  }
    

  /** main routine
   * requires 2 input files to build network 
   */
  def main(args: Array[String]) 
  {

    val start: Instant = Instant.now() //.now(fixedClock)
    logger.info(s"Starting application '${prgName}' at $start.")

    // Google StackDriver logger
    // https://googleapis.dev/python/logging/latest/usage.html#export-log-entries-using-sinks
    // https://cloud.google.com/logging/docs/api/tasks/exporting-logs#logging-create-sink-java

    // neo4j parameters
    // val neo4j_ecrypt = "false"
    // val neo4j_user = "neo4j"
    // var neo4j_pass = "password"
    // var neo4j_host = "34.82.120.135"
    // var neo4j_port = "7687" //bolt port
    // var neo4j_url = s"bolt://${neo4j_host}:${neo4j_port}" //"neo4j://localhost"
    // check arg input
    var DEBUG = 0
    var _set_size = 0.0
    var csvFile = ""
    if (args.length == 0) 
    {
      throw new IllegalArgumentException(
        "At least 1 input (size of data) is required: <_set_size> <IPIP_CSV_PATH[optional]>")
    } 
    else if (args.length > 0 && !args(0).isEmpty()) 
    {
      _set_size = args(0).toDouble // determines how big the final dataset is, 0 to use entire dataset
    }

    if (args.length > 1 && !args(1).isEmpty()) 
    {
      csvFile = args(1)
      DEBUG = 1
      // neo4j_host = "localhost"
      // neo4j_user = "neo4j"
      // neo4j_pass = "admin"
      logger.info(s"Mode: DEBUG")
    }
    else 
    {
        logger.info(s"Mode: RELEASE")
    }

    logger.info(s"Starting " + prgName + " now...")
     /** get the SparkSession, SparkContext, and import spark implicits
     */
    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._
    // this.spark = SparkSession.builder()
    //   .config("spark.neo4j.bolt.encryption", neo4j_ecrypt)
    //   .config("spark.neo4j.bolt.user", neo4j_user)
    //   .config("spark.neo4j.bolt.password", neo4j_pass)
    //   .config("spark.neo4j.bolt.url", neo4j_url)
    //   // .master("local[*]") //"Spark://master:7077"
    //   .appName(prgName)
    //   .getOrCreate()

    // the SparkContext for later use
    // val sc = spark.sparkContext
    // import implicits or this session
    // import spark.implicits._
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

     /** (1) read in the dataframe to Google Big Query table
     *  - be sure to identify depenendecy in Maven
     *  - be sure to add connector config to SparkSession
     *  - be sure to "mvn install" an uberJar
     */
    var tempDF = spark.emptyDataFrame
    if (DEBUG > 0) 
    {
      logger.info(s"Reading data from CSV file '${csvFile}'")

      // the debug CSV has cols(<Array>) as cols(<String>) because Spark
      // cannot write <Array> type to CSV. Let's back that out here...
      tempDF = tempDF.readFromCSV(csvFile)
        .withColumn("goals", split(col("goals_str"), "\\|").cast("array<String>"))
        .drop("goals_str")
      logger.info(s"Data successfully read into Spark DataFrame!")
      tempDF.show()
    }
    else 
    {
      val gbq_table = GCP_PROJECT+":"+BQ_DATASET+"."+BQ_TABLE
      logger.info(s"Reading data from Google BQ table '${gbq_table}'...")
      // read in the BQ table as the master DF and drop NaN/Null
      tempDF = tempDF.readFromBQ(GCP_BUCKET,gbq_table).na.drop()
      //       logger.info(s"BQ Table read into DataFrame...")
      logger.info(s"Data successfully read into Spark DataFrame!")
    }

    var dinDF = tempDF.repartition(numPartitions)
    dinDF.persist(StorageLevel.MEMORY_AND_DISK).count

    // for testing, limit to _set_size records
    // TODO: orderBy(asc()) before sampling to make deterministic?
    if(_set_size > 0.0)
    {
      dinDF = dinDF.orderBy(desc("OPN_Z"))
      dinDF = dinDF.limit(_set_size.toInt)
      logger.info(s"Rows in sampled DF: ${dinDF.count()}")
    }

    dinDF = dinDF.orderBy(asc("uuid"))


    logger.info(s"Number of rows =  ${dinDF.count()}")
    logger.info(s"Number of partitions =  ${dinDF.rdd.getNumPartitions}")
    /**/

    /** (2) create the GraphFrames for each monolayer graph
     * Export the personality factor layers each as a GraphFrame DF object
     *  - it uses the "XXX_Z" input to key the correct layer/column
     *  - analyzes each layer 
     */
    var grOpn = buildMono(dinDF,"OPN_Z",numPartitions,persist=true)//,csv=SAVE_CSV)
    var grCsn = buildMono(dinDF,"CSN_Z",numPartitions,persist=true)//,csv=SAVE_CSV)
    var grExt = buildMono(dinDF,"EXT_Z",numPartitions,persist=true)//,csv=SAVE_CSV)
    var grAgr = buildMono(dinDF,"AGR_Z",numPartitions,persist=true)//,csv=SAVE_CSV)
    var grNeu = buildMono(dinDF,"NEU_Z",numPartitions,persist=true)//,csv=SAVE_CSV)
    /**/
   
    /** (3) Store graph in a neo4j graphDB for later use */
    // TODO: this is a work in progress, it is not working as I had hoped....
    // For now, load CSVs
    /**/

    /** (4) Analyze each layer and prepare for multi-layer analysis */
    /* 
    grOpn = intraLayerAnlaysis(grOpn,"OPN_Z",alg="pagerank")//,csv=SAVE_CSV)
    grCsn = intraLayerAnlaysis(grCsn,"CSN_Z",alg="pagerank")//,csv=SAVE_CSV)
    grExt = intraLayerAnlaysis(grExt,"EXT_Z",alg="pagerank")//,csv=SAVE_CSV)
    grAgr = intraLayerAnlaysis(grAgr,"AGR_Z",alg="pagerank")//,csv=SAVE_CSV)
    grNeu = intraLayerAnlaysis(grNeu,"NEU_Z",alg="pagerank")//,csv=SAVE_CSV)
    */
    /**/


    /** (5) build multi-layer graph */
    // set checkpoint diretory for connected components
    sc.setCheckpointDir("gs://eecs-e6895-bucket/tmp/")
    val gdfs: Seq[(String,GraphFrame)] = Seq(("OPN",grOpn),("CSN",grCsn))
    // var grMulti = buildMulti(grOpn,grCsn,grExt,grAgr,grNeu,numPartitions,persist=true)//,csv=SAVE_CSV)
    var grMulti = buildMulti(gdfs,numPartitions,persist=true)//,csv=SAVE_CSV)

    /** (6) Perform multi-layer analysis */
    // TODO
    grOpn = interLayerAnlaysis(grOpn,"OPN_Z",alg="custom")//,csv=SAVE_CSV)
    /**/

      
    // this persists GraphFrame's edge and vertex DFs. TODO: is this necessary
    // since I've already persisted both DFs?

    // if(DEBUG > 0) {
   

    // logger.info(s"grCsn.edges")
    // // grOpn.edges.orderBy(desc("OPN_Z_dist")).show()
    // grCsn.edges.orderBy(asc("dst")).show(10)
    // logger.info(s"grCsn.edges.count = ${grCsn.edges.count}")
    
    // filter by country
    // logger.info(s"grOpn.country == 'ZAF'")
    // grOpn.vertices.filter("country == 'ZAF'").show()
    // }
    
    /** (5) Store graph in a neo4j graphDB for later use */
    // TODO: this is a work in progress, it is not working as I had hoped....
    // https://github.com/neo4j-contrib/neo4j-spark-connector/blob/master/README.md
    // https://github.com/neo4j-contrib/neo4j-spark-connector/blob/master/src/test/scala/org/neo4j/spark/Neo4jSparkTest.scala#L140
    // https://programminghistorian.org/en/lessons/dealing-with-big-data-and-network-analysis-using-neo4j

    // logger.info(s"Starting neo4j")
    // val neo = Neo4j(sc)


    // Neo4jGraph.saveGraph(sc, xGraph) //, [nodeProp], [relTypeProp (type,prop)], [mainLabelId (label,prop)],[secondLabelId (label,prop)],merge=false) 
    // to load graphframe from neo4j
    // val graphFrame = neo.pattern(("Person","id"),("KNOWS",null), ("Person","id")).partitions(3).rows(1000).loadGraphFrame
    // logger.info(graphFrame.vertices.count)
     
      // // prepare GraphFrame (i.e., relabel)
      // logger.info(s"Convert GraphFrame to GraphX for neo4j")
      // val grOpnX = grOpn.toGraphX
      // logger.info(s"grOpnX.vertices")
      // grOpnX.vertices.take(5).foreach(println(_))
      // logger.info(s"grOpnX.edges")
      // grOpnX.edges.take(5).foreach(println(_))
      
      // // data format...
      // // [10355,[10355,Sherrell,2001-08-14 06:56:46.0,29,F,IND,2.261]
      // // [4663,[4663,Adrienne,2001-07-24 16:19:28.0,29,F,ZAF,0.222])]
      // // Edge(4663,10355,[4663,10355,1.2415])
      // logger.info(s"grOpnX.edges REMAPPED")
      // grOpnX.edges.map(e => (e._1,e._2,e._3(2))).take(5).foreach(println(_))
      // logger.info(s"Saved graph")
      // // quick test
      // val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
      // val graph = Graph.fromEdges(edges,13L)
      // logger.info(s"graph.vertices")
      // graph.vertices.take(5).foreach(println(_))
      // logger.info(s"graph.edges")
      // graph.edges.take(5).foreach(println(_))
      //
      // val graph2 = Graph.fromEdges(grOpnX.edges,13L)
      // logger.info(s"graph2.vertices")
      // graph2.vertices.take(5).foreach(println(_))
      // logger.info(s"graph2.edges")
      // graph2.edges.take(5).foreach(println(_))
      // // Neo4jGraph.saveGraph(sc,graph,"value",("FOOBAR","test"),Option("Foo","id"),Option("Bar","id"),merge = true)
      // val neoRDD = neo.cypher("MATCH (n) RETURN n").partitions(5).batch(10000).loadRowRdd//loadDataFrame(("test","test2"))
      // logger.info(s"neoRDD")
      // logger.info(s"$neoRDD")
      // neoRDD.take(5).foreach(println(_))
      
      // Neo4jGraph.saveGraph(sc,graph2,"user",("LINK","OPN_Z_fit"),Option("src","id"),Option("dst","id"),merge = true)
      // val neo4jTest = neo.cypher("MATCH (n) RETURN n;").loadRdd[Long]
      // logger.info(s"neo4jTest=$neo4jTest")
      // Neo4jGraph.saveGraph(sc,grOpnX,merge=true)

      // val Neo4jDataFrame.mergeEdgeList(
      //     opnDF_e,                              // Dataframe 
      //     ("src", Seq("src")),                
      //     ("OPN_mean", Seq("OPN_mean")),
      //     ("dst", Seq("dst"))
      //   )
  


    /** To save data as CSV on GCP, uncomment [START HERE]*/
   
    
    // // TODO: figure out why goals is in a weird format...
    // grOpn.vertices.orderBy(asc("id")).withColumn("goals_str", flatten($"goals")).show() // concat_ws(" ", $"rate_plan_code")
    // grOpn.vertices.orderBy(asc("id")).printSchema() // concat_ws(" ", $"rate_plan_code")
    // grOpn.vertices.orderBy(asc("id")).withColumn("goals_str", explode($"goals")).select($"goals_str").show() // concat_ws(" ", $"rate_plan_code")

    // flatten($"subjects")
    // convert cols(<Array>) to cols(<String>) with "|" delimiter because Spark
    // cannot write <Array> type to CSV. 
    // logger.info(s"grOpn.write.format('csv')")
    // grOpn.vertices.orderBy(asc("id"))
        // .withColumn("goals_str", array_join($"goals", "|")) // concat_ws(" ", $"rate_plan_code")
        // .withColumn("goals_str", concat_ws("|",$"goals")) // concat_ws(" ", $"rate_plan_code")
        // .withColumn("goals_str", flatten($"goals"))
    
    // [START HERE]
   
    /** Saving to CSV Files */   
    // if (SAVE_CSV)
    // {
    //   val gcp_path = s"gs://${GCP_BUCKET}/output"
    //   logger.info(s"Saving data as CSV to $gcp_path")

    //   // var file_path = gcp_path+"/grOpn_vert_${_set_size.toInt}.csv"
    //   // gdfToBucket(grOpn.vertices,gcp_path,"Opn_vert_${_set_size.toInt}.csv")
    //   var file_path = s"${gcp_path}/Opn_vert_${_set_size.toInt}.csv"
    //   grOpn.vertices.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // grOpn.vertices.orderBy(asc("id"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(file_path)
    //   // logger.info(s"grOpn.vertices.count = ${grOpn.vertices.count}")
    //   file_path = s"${gcp_path}/Opn_edges_${_set_size.toInt}.csv"
    //   grOpn.edges.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // grOpn.edges.orderBy(asc("dst"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grOpn_edge.csv")
    //   // logger.info(s"Saved 'grOpn_edge' to $file_path")
    //   // logger.info(s"grOpn.edges.count = ${grOpn.edges.count}")

    //   file_path = s"${gcp_path}/Csn_vert_${_set_size.toInt}.csv"
    //   grCsn.vertices.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // logger.info(s"grCsn.write.format('csv')")
    //   // grCsn.vertices.orderBy(asc("id"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grCsn_vert_${_set_size.toInt}.csv")
    //   file_path = s"${gcp_path}/Csn_edges_${_set_size.toInt}.csv"
    //   grCsn.edges.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // grCsn.edges.orderBy(asc("dst"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grCsn_edge.csv")

    //   file_path = s"${gcp_path}/Ext_vert_${_set_size.toInt}.csv"
    //   grExt.vertices.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // logger.info(s"grExt.write.format('csv')")
    //   // grExt.vertices.orderBy(asc("id"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grExt_vert_${_set_size.toInt}.csv")

    //   file_path = s"${gcp_path}/Ext_edges_${_set_size.toInt}.csv"
    //   grExt.edges.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // grExt.edges.orderBy(asc("dst"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grExt_edge.csv")

    //   file_path = s"${gcp_path}/Agr_vert_${_set_size.toInt}.csv"
    //   grAgr.vertices.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // logger.info(s"grAgr.write.format('csv')")
    //   // grAgr.vertices.orderBy(asc("id"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grAgr_vert_${_set_size.toInt}.csv")

    //   file_path = s"${gcp_path}/Agr_edges_${_set_size.toInt}.csv"
    //   grAgr.edges.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // grAgr.edges.orderBy(asc("dst"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grAgr_edge.csv")
      
    //   file_path = s"${gcp_path}/Neu_vert_${_set_size.toInt}.csv"
    //   grNeu.vertices.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // logger.info(s"grNeu.write.format('csv')")
    //   // grNeu.vertices.orderBy(asc("id"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grNeu_vert_${_set_size.toInt}.csv")

    //   file_path = s"${gcp_path}/Neu_edges_${_set_size.toInt}.csv"
    //   grNeu.edges.writeToCSV(file_path)
    //   logger.info(s"Saved $file_path")
    //   // grNeu.edges.orderBy(asc("dst"))
    //   //     .write.format("csv")
    //   //     .option("header", "true")
    //   //     .mode("overwrite")
    //   //     .save(gcp_path+"/grNeu_edge.csv")
    // }

    // val DF = loader.loadDataFrame
    // val sc = spark.sparkContext
    // val neo = Neo4j(sc)
    // if (DEBUG > 0) {
    //   // if DEBUG, connect locally
    //  

    // (6) analysis
    // Run PageRank until convergence to tolerance "tol".
    // val prOpn = grOpn.pageRank.resetProbability(0.15).tol(0.01).run()
    // Run PageRank for a fixed number of iterations.
    
    // OPN
    // logger.info(s"Starting OPN pageRank")
    // var prOpn = grOpn.pageRank.resetProbability(0.15).maxIter(10).run()
    // prOpn.vertices.orderBy(asc("id")).show(10)
    // prOpn.edges.orderBy(asc("dst")).show(10) 
    // // prOpn.vertices.select("id", "pagerank").orderBy(desc("pagerank")).show()
    // // prOpn.edges.select("src", "dst", "weight").orderBy(desc("weight")).show() 
    // // prOpn.edges.select("src", "dst", "weight").orderBy(asc("dst")).show(10) 
    // // CSN
    // logger.info(s"Starting CSN pageRank")
    // var prCsn = grCsn.pageRank.resetProbability(0.15).maxIter(10).run()
    // prCsn.vertices.orderBy(asc("id")).show(10)
    // prCsn.edges.orderBy(asc("dst")).show(10) 
    // // EXT
    // logger.info(s"Starting EXT pageRank")
    // var prExt = grExt.pageRank.resetProbability(0.15).maxIter(10).run()
    // prExt.vertices.orderBy(asc("id")).show(10)
    // prExt.edges.orderBy(asc("dst")).show(10) 
    // // AGR
    // logger.info(s"Starting AGR pageRank")
    // var prAgr = grAgr.pageRank.resetProbability(0.15).maxIter(10).run()
    // prAgr.vertices.orderBy(asc("id")).show(10)
    // prAgr.edges.orderBy(asc("dst")).show(10) 
    // // NEU
    // logger.info(s"Starting NEU pageRank")
    // var prNeu = grNeu.pageRank.resetProbability(0.15).maxIter(10).run()
    // prNeu.vertices.orderBy(asc("id")).show(10)
    // prNeu.edges.orderBy(asc("dst")).show(10) 


    // for connected components, we need a Spark checkpoint directory
    // sc.setCheckpointDir("gs://eecs-e6895-bucket/tmp/")
    // logger.info(s"Opn connected components results")
    // var ccOpn = grOpn.connectedComponents.run() 
    // // ccOpn.select("id", "component").orderBy(desc("component")).show()
    // ccOpn.orderBy(asc("id")).show(10)
    // logger.info(s"Csn conn. comp. results")
    // var ccCsn = grCsn.connectedComponents.run() 
    // ccCsn.orderBy(asc("id")).show(10)
    
    
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