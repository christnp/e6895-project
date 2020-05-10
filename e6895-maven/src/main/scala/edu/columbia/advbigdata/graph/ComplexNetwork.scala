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
import org.apache.spark.mllib.linalg.{Vector,Vectors,Matrix,Matrices}
import org.apache.spark.mllib.linalg.distributed._ //{RowMatrix,IndexedRow,IndexedRowMatrix,BlockMatrix,MatrixEntry}

// spark-bigquery_2.11 artifacts
import com.google.cloud.spark.bigquery._
// import com.google.auth.oauth2.GoogleCredentials

// spark-graphx_2.11 artifacts
import org.apache.spark.graphx._

// graphframes_2.11 artifact
import org.graphframes._

// breeze_2.11 artifact
import breeze.linalg.{pinv,DenseMatrix => BDM,DenseVector => BDV}

// neo4j-spark-connector artifact
import org.neo4j.spark._
import org.neo4j.spark.dataframe._

// org.scala-lang artifacts
import scala.util.Random 
import scala.util.control.Breaks
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
import com.github.kobeliu85.CloudCP
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
    * @param rowMat
    */
  //  def normRowMat(rowMat:IndexedRowMatrix, L:Vector) : IndexedRowMatrix = {

  //   val map_M = rowMat.rows.map(x =>
  //     (x.index, x.vector.toArray)).mapValues(x => x:/L)

  //   val NL_M:IndexedRowMatrix = new IndexedRowMatrix(map_M
  //     .map(x => IndexedRow(x._1, Vectors.dense(x._2.toArray))))

  //   NL_M
  // }

  /** Frobenius Norm
    * @param rowMat an indexed row matrix
    * 
    * Ref: https://stackoverflow.com/questions/29869567/spark-distributed-matrix-multiply-and-pseudo-inverse-calculating
    */
  def frNorm(rowMat : IndexedRowMatrix) : Vector = {
    
    return Vectors.dense(rowMat.toRowMatrix()
      .computeColumnSummaryStatistics().normL2.toArray)

  }

  /** Moore-Penrose Pseudo-Inverse
    *  - this implementiaton of the Hardamard (element-wise) matrix
    *    multiplication uses RDD-like structures
    * @param rowMat indexedRowMatrix matrix
    */
  def mpPseudoInv(rowMat: IndexedRowMatrix) : Matrix = {

    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    // TODO: this seems expensive. 
    val localMat = rowMat.toBlockMatrix.toLocalMatrix
    val rows = localMat.numRows
    val cols = localMat.numCols
    val denseMat = new BDM[Double](rows,cols,localMat.toArray)
    // val inverse = CloudCP.BDMtoMatrix(pinv(denseMat))
    
    
    return Matrices.dense(rows, cols, pinv(denseMat).data)    
  }

  /** Khatri-Rao Product
    * 
    *
    * @param left
    * @param right
    */
  def krProduct(lkr:IndexedRowMatrix,rkr:IndexedRowMatrix) : IndexedRowMatrix = {

    val tmpLKR = lkr.rows.map{case IndexedRow(i,vec) => (i,vec)} // (row,(L,vector))
    val tmpRKR = rkr.rows.map{case IndexedRow(i,vec) => (i,vec)} // (row,(R,vector))
    val tmpKR = tmpLKR.cartesian(tmpRKR) // (left(0:i),right(0:j))
      .map{case ((li,lv),(ri,rv)) => lv.toArray.zip(rv.toArray).map{ case (a, b) => a * b }}
      .zipWithIndex()
      .map{ case(arr,i) => IndexedRow(i,Vectors.dense(arr.map(_.toDouble)))}

    return new IndexedRowMatrix(tmpKR)
  }


  /** Hardamard product of two IndexedRowMatrices
    *  - this implementiaton of the Hardamard (element-wise) matrix
    *    multiplication uses RDD-like structures
    * @param left left IndexedRowMatrix
    * @param right right IndexedRowMatrix
    */
  def hmProduct(
    left: IndexedRowMatrix, 
    right: IndexedRowMatrix
  ) : CoordinateMatrix = {
    
    //IndexedRowMatrix.rows
    val tmp = (left.rows ++ right.rows)
      .flatMap{case IndexedRow(i,vec) => 
          vec.toArray.zipWithIndex.map{case (v,j) => ((i,j),v)}
        }
      .reduceByKey(_*_) // TODO: should I sortBy(key)?
      .map{case(k,v) => MatrixEntry(k._1,k._2,v)}

    return new CoordinateMatrix(tmp)
  }

  /**
    * 
    *
    * @param dim dimension of the factor matrix
    * @param rank desired rank of the factor matrix
    */
  def coordFactorInit(dim: Long, rank: Long) : CoordinateMatrix = {
    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    // random Int generator
    def elemInit = scala.util.Random.nextInt(2) // randomly choose between [0,2) (i.e., 0 or 1)
    // A factor matrix initialization
    val tmpFactRDD = sc.parallelize(Array.fill(dim.toInt,rank.toInt){elemInit})
      .zipWithIndex
      .flatMap{ case(arr,i) => arr.zipWithIndex.map{case(v,j) => MatrixEntry(i, j, v.toDouble)}}

    return new CoordinateMatrix(tmpFactRDD)    
  }


   /**
    * 
    *
    * @param dim dimension of the factor matrix
    * @param rank desired rank of the factor matrix
    */
  def rowFactorInit(dim: Long, rank: Long) : IndexedRowMatrix = {
    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    // random Int generator
    def elemInit = scala.util.Random.nextInt(2) // randomly choose between [0,2) (i.e., 0 or 1)
    // A factor matrix initialization
    val tmpFactRDD = sc.parallelize(Array.fill(dim.toInt,rank.toInt){elemInit})
      .zipWithIndex
      .map{ case(arr,i) => IndexedRow(i,Vectors.dense(arr.map(_.toDouble)))}

    return new IndexedRowMatrix(tmpFactRDD)
  }

  /**
    * 3-Way Tensor CPD using Spark
    * ref: Kolda, et. al., "Tensor Decomposition and Applications"
    *
    * @param dim dimension of the factor matrix
    * @param rank desired rank of the factor matrix
    */
  def tensor3CPD(
    X1    : IndexedRowMatrix, 
    X2    : IndexedRowMatrix, 
    X3    : IndexedRowMatrix, 
    rank  : Int = 2,
    iter  : Int = 100,
    tol   : Double = 0.0001
  ) : IndexedRowMatrix = {
    
    val tensor3CPD_start = Instant.now()
    var timer_start = Instant.now()

    val spark = this.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    /**
      * Initialize parameters
      */
    // 3-way tensor dimensions
    val A1_dim = X1.numRows //N x N*L
    val A2_dim = X2.numRows //N x N*L
    val A3_dim = X3.numRows //L x N*N
   
    // initialize the factors
    val A2 = rowFactorInit(A2_dim,rank)
    val A3 = rowFactorInit(A3_dim,rank)

    // we want to make sure the dimensions are correct
    require((A2.numRows == A2_dim && A2.numCols == rank), "A2 dimensions incorrect")
    require((A3.numRows == A3_dim && A3.numCols == rank), "A3 dimensions incorrect")

    logger.info(s"Factor Matrix dimensions: ")
    logger.info(s"  A1 (nodes):  ${A1_dim}x${rank}")
    logger.info(s"  A2 (nodes):  ${A2_dim}x${rank}")
    logger.info(s"  A3 (layers): ${A3_dim}x${rank}")

    ///////////////////////// for debugging ///////////////////////////////////
    // C^T C, B^T B are the Garmian matricies! 
    timer_start = Instant.now()
    // var AtA_brz = A1.computeGramianMatrix()
    var BtB_brz = A2.computeGramianMatrix()
    var CtC_brz = A3.computeGramianMatrix()
    // intermediate step - convert to Breeze DenseMatrix(BDM)
    val M1M:BDM[Double] = new BDM[Double](CtC_brz.numRows,CtC_brz.numCols,CtC_brz.toArray)
    val M2M:BDM[Double] = new BDM[Double](BtB_brz.numRows,BtB_brz.numCols,BtB_brz.toArray)
    val tmp_breeze = M1M:*M2M
    logger.info(s"Breeze hardamard matrix multiplication took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
    println(s"tmp_breeze: rows=${tmp_breeze.rows}, cols=${tmp_breeze.cols}")
    println(tmp_breeze)
    println("\n")
    timer_start = Instant.now()
    val tmp_pinv = pinv(tmp_breeze)
    logger.info(s"Breeze pseudo inverse took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
    println(s"tmp_breeze: rows=${tmp_pinv.rows}, cols=${tmp_pinv.cols}")
    println(tmp_pinv)
    ///////////////////////////////////////////////////////////////////////////
    
    /**  A^TA, B^TB, C^TC Hardamard products (element-wise multiplication)
      * 
      */
    val blkA3 = A3.toBlockMatrix
    val blkA2 = A2.toBlockMatrix
    // val blkA1 = A1.toBlockMatrix

    timer_start = Instant.now() 
    /////
    val A3tA3 = (blkA3.transpose).multiply(blkA3).toIndexedRowMatrix
    val A2tA2 = (blkA2.transpose).multiply(blkA2).toIndexedRowMatrix
    // val AtA = (blkA1.transpose).multiply(blkA1).toIndexedRowMatrix
    val A3hA2 = hmProduct(A3tA3,A2tA2) // hardamard product
    /////
    logger.info(s"Hardamard product A3hA2 with A3tA3/A2tA2 transposes took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")

    /** Moore-Penrose pseudo-inverse using Breeze LinAlg package
      *  - output is a Matrix, perfect for IndexedRowMatrix multiply!
      * 
      */ 
    timer_start = Instant.now() 
    /////
    val A3hA2_pinv = mpPseudoInv(A3hA2.toIndexedRowMatrix) // returns a Matrix!
    /////

    /** Khatri-Rao Product (CkB=NLxR,CkA=NLxR,BkA=NNxR)
      * 
      */
    timer_start = Instant.now() 
    /////
    val A3kA2 = krProduct(A3,A2)
    /////
    logger.info(s"Khatri-Rao product A3kA2 took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")


    /** Multiply tensor, Khatri-Rao, and Hardamard 
      * 
      */
    timer_start = Instant.now() 
    /////
    val Z_n = A3kA2.multiply(A3hA2_pinv)
    var A1 = X1.toBlockMatrix.multiply(Z_n.toBlockMatrix).toIndexedRowMatrix
    /////
    logger.info(s"Multiplying tensor,krProduct, and hmProduct took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
     
    val lambda = CloudCP.UpdateLambda(A1,0) // 0=L2norm Euclidean norm (Frobenius Norm); 1=max
    // val lambda = frNorm(A1)
    val A1_norm = CloudCP.NormalizeMatrix(A1,lambda)

    ////////////////// DEBUG OUTPUT
    // X1
    println(s"mode1")
    X1.rows.collect.foreach(println)
    println("\n")
    // X2
    println(s"mode2")
    X2.rows.collect.foreach(println)
    println("\n")
    // X3
    println(s"mode3")
    X3.rows.collect.foreach(println)
    println("\n")
    // A1
    // println(s"A1 as IndexedRowMatrix")
    // A1.rows.collect.foreach(println)
    // println("\n")
    // A2
    println(s"A2 as IndexedRowMatrix")
    A2.rows.collect.foreach(println)
    println("\n")
    // A3
    println(s"A3 as IndexedRowMatrix")
    A3.rows.collect.foreach(println)
    println("\n")
    // A1tA1
    // TBD?
    // A2tA2
    println(s"A2tA2 as IndexedRowMatrix")
    A2tA2.rows.collect.foreach(println)
    println("\n")
    // A3tA3
    println(s"A2tA2 as IndexedRowMatrix")
    A2tA2.rows.collect.foreach(println)
    println("\n")
    // A3hA2
    println(s"A3hA2: rows=${A3hA2.numRows}, cols=${A3hA2.numCols}")
    A3hA2.entries.collect.foreach(println)
    println("\n")
    // A3hA2_pinv
    println(s"A3hA2_pinv Pseudo Inverse")
    println(s"$A3hA2_pinv")
    println("\n")
    // A3kA2
    println(s"A3kA2")
    A3kA2.rows.collect.foreach(println)
    println("\n")
    // Z_n    
    println(s"Z_n")
    Z_n.rows.collect.foreach(println)
    println("\n")
    // A1
    println(s"A1")
    A1.rows.collect.foreach(println)
    println("\n")
    // lambda
    println(s"lambda")
    println(lambda)
    println("\n")
    // A1_norm
    println(s"A1_norm")
    A1_norm.rows.collect.foreach(println)
    println("\n")
    ///////////////////////////////

    // relative residual norm ||X-X^||/||X||
    // sum of error delta per fact. i.e., sumErr = errX1 + errX2 + errX3
    // function finished  
    logger.info(s"tensor3CPD_start() finished in ${Duration.between(tensor3CPD_start,Instant.now()).toMillis()} ms")
   
    // val test = frNorm(A1)
    // prtinln()

    return A1
  }

//   /**
//     * 3-Way Tensor CPD using Spark
//     *
//     * @param dim dimension of the factor matrix
//     * @param rank desired rank of the factor matrix
//     */
//   def tensor3CPD(
//     X1: IndexedRowMatrix, 
//     X2: IndexedRowMatrix, 
//     X3: IndexedRowMatrix, 
//     rank : Int = 2,
//     iterations : Int = 100,
//     tolerance : Double = 0.0001
//   ) = {
//     // val inputFile = "/users/cqwcy201101/Desktop/test.md"
//     // val output = "/users/cqwcy201101/Desktop/result/"

//     /*--------------------------------
//     val inputm1 =  "/Users/cqwcy201101/Desktop/MA.md"
//     val inputm2 =  "/Users/cqwcy201101/Desktop/MB.md"
//     */
//     // val time_s:Double=System.nanoTime()
//     // val conf = new SparkConf().setAppName("TensorCP").setMaster("local").set("spark.ui.port","4040")
//     // val sc = new SparkContext(conf)
//     // val X1 = sc.textFile(inputFile)

//     val spark = this.sparkSession
//     val sc = spark.sparkContext
// //     import spark.implicits._
    
//     // val X1 = CloudCP.readFile(X1).cache()
//     // val X1_RDD = CloudCP.IndexedRowMatrixToRDD_V(X1)
//     val X1_RDD = X1.toRowMatrix.rows
//     // val X2_RDD = CloudCP.IndexedRowMatrixToRDD_V(X2)
//     val X2_RDD = X2.toRowMatrix.rows
//     // val X3_RDD = CloudCP.IndexedRowMatrixToRDD_V(X3)
//     val X3_RDD = X3.toRowMatrix.rows

//     println("X1_RDD")
//     X1_RDD.collect.foreach(println)
// /*
//     //-----------------------------
//     val MAdata = sc.textFile(inputm1)
//       .map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

//     val MBdata = sc.textFile(inputm2)
//       .map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
//     //----------------------------
// */
//     // val SizeVecX1 = CloudCP.RDD_VtoRowMatrix(X1)
//     val SizeVecX1 = X1.toRowMatrix
//       .computeColumnSummaryStatistics().max
//     // val SizeVecX2 = CloudCP.RDD_VtoRowMatrix(X2)
//     val SizeVecX2 = X2.toRowMatrix
//       .computeColumnSummaryStatistics().max
//     // val SizeVecX3 = CloudCP.RDD_VtoRowMatrix(X3)
//     val SizeVecX3 = X3.toRowMatrix
//       .computeColumnSummaryStatistics().max

//     val SizeA:Long = SizeVecX1.apply(0).toLong+1
//     val SizeB:Long = SizeVecX2.apply(1).toLong+1
//     val SizeC:Long = SizeVecX3.apply(2).toLong+1
    
//     println(s"SizeVecX1=$SizeVecX1,SizeVecX1.class=${SizeVecX1.getClass}")    
//     println(s"SizeA=$SizeA,SizeA.class=${SizeA.getClass}")

//     // rank
//     // val rank:Int = 2
//     val Dim_1:Int = 0 // TODO: What do these do? NC
//     val Dim_2:Int = 1
//     val Dim_3:Int = 2

//     // factor initialization 
//     // TODO: replace with my random [0,1] generator?
//     var MA:IndexedRowMatrix = CloudCP.InitialIndexedRowMatrix(SizeA,rank,sc)
//     var MB:IndexedRowMatrix = CloudCP.InitialIndexedRowMatrix(SizeB,rank,sc)
//     var MC:IndexedRowMatrix = CloudCP.InitialIndexedRowMatrix(SizeC,rank,sc)

//     println("MA")
//     MA.rows.collect.foreach(println)

//     var N:Int = 0

// /*
//     var MA:IndexedRowMatrix = new IndexedRowMatrix(MAdata.zipWithIndex().map{case (x,y) => IndexedRow(y,x)})
//     var MB:IndexedRowMatrix = new IndexedRowMatrix(MBdata.zipWithIndex().map{case (x,y) => IndexedRow(y,x)})
//     var MC:IndexedRowMatrix = new IndexedRowMatrix(MAdata.zipWithIndex().map{case (x,y) => IndexedRow(y,x)})
// */

//     // calculate the initial "M^T M" components
//     var ATA = CloudCP.Compute_MTM_RowMatrix(MA)
//     var BTB = CloudCP.Compute_MTM_RowMatrix(MB)
//     var CTC = CloudCP.Compute_MTM_RowMatrix(MC)

//     println(s"ATA.class = ${ATA.getClass}, ATA=\n$ATA")

//     // initialize fit parameters
//     var Lambda:BDV[Double] = BDV.zeros(rank)
//     var fit:Array[Double] = Array(0.0, 0.0, 0.0)
//     var prev_fit:Array[Double] = Array(0.0, 0.0, 0.0)
//     var val_fit:Array[Double] = Array(0.0, 0.0, 0.0)

//     println(s"Lambda.class = ${Lambda.getClass}, Lambda=$Lambda")
//     println(s"fit.class = ${fit.getClass}, fit=$Lambda")
//     println(s"prev_fit.class = ${prev_fit.getClass}, prev_fit=$Lambda")
//     println(s"val_fit.class = ${val_fit.getClass}, val_fit=$Lambda")

//     /*
//         val testM1 = CalculateM1(X1,MB,MC,Dim_1,SizeA,rank,sc)
//         testM1.rows.collect().foreach(println)
//         val testM2 = CalculateM2(MB,MC)
//         println(testM2)
//         val testM1M1 = testM1.multiply(testM2)
//         testM1M1.rows.collect().foreach(println)

//         Lambda = UpdateLambda(testM1M1)
//         //val RDDLambda = sc.parallelize(Vector(Lambda))
//         //var boradambda = sc.broadcast(RDDLambda.collect())

//         println(Lambda)

//         val testM1M1_2 = testM1M1.rows.map(x => (x.index, BDV[Double](x.vector.toArray))).mapValues(x=> (x :/ Lambda))

//           //IndexedRow(x.index, Vectors.dense(((BDV[Double](x.vector.toArray)) :/ Lambda ).toArray)))
//         testM1M1_2.collect().foreach(println)

//         val testM1M2_3 = UpdateMatrix(X1,Lambda,MB,MC,Dim_1,SizeA,rank,sc)
//         testM1M2_3.rows.collect().foreach(println)
//     */


//     /*
//         val testB1 = CalculateM1(X1,MC,MA,Dim_2,SizeB,rank,sc)
//         testB1.rows.collect().foreach(println)
//         val testB2 = testB1.multiply(CalculateM2(MC,MA))
//         testB2.rows.collect().foreach(println)

//         for (i <- 0 until rank){
//           Lambda = UpdateLambda(testB2,i)
//           MB = UpdateMatrix(X1,Lambda,MC,MA,Dim_2,SizeB,rank,sc)

//         }
//         println(Lambda)
//         MB.rows.collect().foreach(println)
//     */


//     // loop until max iterationsions is reached OR error is within tolerance
//     var stage = 0
//     val loop = new Breaks
//     loop.breakable
//     {
//       for (i <- 0 until  iterations)
//       {
//         println("\n")
//         logger.info(s"Tensor CPD Stage $N")

//         ///////////////////////////// A
//         println("\n")
//         println("<<<< Starting MA\n")

//         MA = CloudCP.UpdateMatrix(X1_RDD,MB,MC,Dim_1,SizeA,rank,sc)
        
//         println("MA after UpdateMatrix")
//         MA.rows.collect.foreach(println) 

//         Lambda = CloudCP.UpdateLambda(MA,i)
//         MA = CloudCP.NormalizeMatrix(MA,Lambda)
//         ATA = CloudCP.Compute_MTM_RowMatrix(MA)
//         println("MA Done >>>>\n")
//         println("MA after NormalizeMatrix")
//         MA.rows.collect.foreach(println)  
//         println(s"ATA.class = ${ATA.getClass}, ATA=\n$ATA")
//         println(s"Lambda.class = ${Lambda.getClass}, Lambda=$Lambda")

//         ///////////////////////////// B
//         println("\n")
//         println("<<<< Starting MB\n")
//         MB = CloudCP.UpdateMatrix(X2_RDD,MC,MA,Dim_2,SizeB,rank,sc)
        
//         println("MB after UpdateMatrix")
//         MB.rows.collect.foreach(println)

//         Lambda =CloudCP.UpdateLambda(MB,i)
//         MB = CloudCP.NormalizeMatrix(MB,Lambda)
//         BTB = CloudCP.Compute_MTM_RowMatrix(MB)

//         println("MB Done >>>>\n")
//         println("MB after NormalizeMatrix")
//         MB.rows.collect.foreach(println)  
//         println(s"BTB.class = ${BTB.getClass}, BTB=\n$BTB")
//         println(s"Lambda.class = ${Lambda.getClass}, Lambda=$Lambda")

//         ///////////////////////////// C
//         println("\n")
//         println("<<<< Starting MC\n")

//         MC= CloudCP.UpdateMatrix(X3_RDD,MA,MB,Dim_3,SizeC,rank,sc)
//         Lambda = CloudCP.UpdateLambda(MC,i)
//         MC = CloudCP.NormalizeMatrix(MC,Lambda)
//         CTC= CloudCP.Compute_MTM_RowMatrix(MC)
       
//         println("MC Done >>>>\n")
//         println("MC after NormalizeMatrix")
//         MC.rows.collect.foreach(println)  
//         println(s"CTC.class = ${CTC.getClass}, CTC=\n$CTC")
//         println(s"Lambda.class = ${Lambda.getClass}, Lambda=$Lambda")

//         // NC: updated 5/9/20 to work with 3-way tensor
//         prev_fit = fit
//         fit(0) = CloudCP.ComputeFit(X1_RDD,Lambda,MA,MB,MC,ATA,BTB,CTC)
//         fit(1) = CloudCP.ComputeFit(X2_RDD,Lambda,MA,MB,MC,ATA,BTB,CTC)
//         fit(2) = CloudCP.ComputeFit(X3_RDD,Lambda,MA,MB,MC,ATA,BTB,CTC)
//         val_fit(0) = CloudCP.FitAbs(fit(0) - prev_fit(0))
//         val_fit(1) = CloudCP.FitAbs(fit(1) - prev_fit(1))
//         val_fit(2) = CloudCP.FitAbs(fit(2) - prev_fit(2))

//         println("\n")
//         println("<<<< Fit statistics")
//         println(s"fit.class = ${fit.getClass}, fit=$Lambda")
//         println(s"prev_fit.class = ${prev_fit.getClass}, prev_fit=$Lambda")
//         println(s"val_fit.class = ${val_fit.getClass}, val_fit=$Lambda")

//         N = N +1
//         // NC: updated 5/9/20 to use mean tolerance
//         if ((val_fit.sum / val_fit.length) < tolerance)
//           loop.break
//       }
//     }

//     //---------------
//     val time_e:Double=System.nanoTime()

//     //---------------

//     println(s"\nMA, MA.claLambdass = ${MA.getClass}")
//     MA.rows.collect().foreach(println)
//     println(s"\nMB, MB.class = ${MB.getClass}")
//     MB.rows.collect().foreach(println)
//     println(s"\nMC, MC.class = ${MC.getClass}")
//     MC.rows.collect().foreach(println)
    
    
//     // CloudCP.OutputResult(MA,output+"MatrixA")
//     // CloudCP.OutputResult(MB,output+"MatrixB")
//     // CloudCP.OutputResult(MC,output+"MatrixC")

//     println(s"val_fit = $val_fit")
//     println(s"N = $N")
//     println(s"Lambda = $Lambda")

//     println("Running time is:")
//     println((time_e-time_s)/1000000000+"s\n")

    // return (MA,MB,MC)
  // }
   /**
    * Function to ingest a DataFrame with layer column name identified
    * and return a GraphFrame object. The idea is that multiple layers
    * can be generated using the same process because each layer utilizes
    * similar data (i.e., name, uuid, location, etc.). 
    *
    *  @param gdfs the source GraphFrames as a Sequence with String Name
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

    // use for task timing
    var timer_start: Instant = Instant.now() 

    // Tensor parameters (some are placeholders)
    var N_dim:Long = 0           // A,B dim = N_dim (node communities)
    var L_dim:Long = gdfs.length.toLong // C dim = L_dim (layers)
    
    /** 
      * build adjacency matrix 
      * */
    // store each mode MatrixEntry in a sequence
    var mode1Seq = Seq[RDD[MatrixEntry]]()
    var mode2Seq = Seq[RDD[MatrixEntry]]()
    var mode3Seq = Seq[RDD[MatrixEntry]]()
    gdfs.zipWithIndex.foreach{ case(layer_gdf,layer_num) =>

      val layer = layer_gdf._1
      val ogdf = layer_gdf._2

      logger.info(s"Adding '${layer}' layer...")      
      val V_o = ogdf.vertices
      val E_o = ogdf.edges
      
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

      logger.info(s"${layer} Adjacency DF")      
      adjDF.show()
      
      /** Convert the DataFrame to an RDD based adjacency matrix and
       * calculate the mode-n matricizations (unfoldings)
       * - refer to Kolda, et. al, "Tensor Decompositions and Applications"
      */
      // get Adjacency matrix columns
      val matCols = adjCols.map(col(_))
      // set matrix dimension values
      N_dim = adjDF.count
      val L_num = layer_num

      val startMat: Instant = Instant.now() 
      val adjRDD = adjDF.select(array(matCols:_*).as("arr")).as[Array[Double]].rdd
      // Mode-1 Matricization
      val tmpOneRDD = adjRDD.zipWithIndex
        .flatMap{ case(arr,i) => arr.zipWithIndex.map{case(v,j) => MatrixEntry(i, L_num*N_dim+j, v.toDouble)}}
      mode1Seq = mode1Seq ++ Seq(tmpOneRDD)
      // Mode-2 Matricization (i.e., flip i and j (transpose) of mode-1)
      val tmpTwoRDD = adjRDD.zipWithIndex
        .flatMap{ case(arr,i) => arr.zipWithIndex.map{case(v,j) => MatrixEntry(j, L_num*N_dim+i, v.toDouble)}}
      mode2Seq = mode2Seq ++ Seq(tmpTwoRDD)
      // Mode-3 Matricization (i.e., rows=fibers (L_num-dim), columns=(i,j) element)
      val tmpThreeRDD = adjRDD.zipWithIndex
        .flatMap{ case(arr,i) => arr.zipWithIndex.map{case(v,j) => MatrixEntry(L_num, i*N_dim+j, v.toDouble)}}
      mode3Seq = mode3Seq ++ Seq(tmpThreeRDD)

      logger.info(s"Layer ${layer} matricization took ${Duration.between(startMat,Instant.now()).toMillis()} ms")
    
    } // end foreach GraphFrame

    /**
      * Combine the mode-n matrices for each layer and create a CoordinateMatrix
      */
    var unionStart: Instant = Instant.now() 
    val mode1Rdd = sc.union(mode1Seq)
    logger.info(s"Mode-1 Union took: ${Duration.between(unionStart,Instant.now()).toMillis()} ms")
    val mode1Mat = new CoordinateMatrix(mode1Rdd) // assert: dim = N x N*L

    unionStart = Instant.now() 
    val mode2Rdd = sc.union(mode2Seq)
    logger.info(s"Mode-2 Union took: ${Duration.between(unionStart,Instant.now()).toMillis()} ms")
    val mode2Mat = new CoordinateMatrix(mode2Rdd) // assert: dim = N x N*L

    unionStart = Instant.now() 
    val mode3Rdd = sc.union(mode3Seq)
    logger.info(s"Mode-3 Union took: ${Duration.between(unionStart,Instant.now()).toMillis()} ms")
    val mode3Mat = new CoordinateMatrix(mode3Rdd) // assert: dim = L x N*N

    //////// DEBUG OUTPUT
    println(s"mode1Mat: numRows=${mode1Mat.numRows}, numCols=${mode1Mat.numCols}\n")
    println(s"mode1Mat as IndexedRowMatrix\n")
    mode1Mat.toIndexedRowMatrix.rows.collect.foreach(println)
    println(s"\n")

    println(s"mode2Mat: numRows=${mode2Mat.numRows}, numCols=${mode2Mat.numCols}\n")
    println(s"mode2Mat as IndexedRowMatrix\n")
    mode2Mat.toIndexedRowMatrix.rows.collect.foreach(println)
    println(s"\n")

    println(s"mode3Mat: numRows=${mode3Mat.numRows}, numCols=${mode3Mat.numCols}\n")
    println(s"mode3Mat as IndexedRowMatrix\n")
    mode3Mat.toIndexedRowMatrix.rows.collect.foreach(println)
    println(s"\n")

   /**
     * Tensor factorization via CP decomposition of rank R
     * A = edges into node i, N x R (number of nodes)
     * B = edges out of node j, N x R (number of nodes)
     * C = personality rank, L x R (number of FFM layers)
     */
    
    // tensor parameters
    val R = 2 // rank/# of components

    // trying this I found on github
    val facts = tensor3CPD(mode1Mat.toIndexedRowMatrix,
                            mode2Mat.toIndexedRowMatrix,
                            mode3Mat.toIndexedRowMatrix,
                            rank=R,
                            iter=5,
                            tol=0.1)

    println(s"facts")
    facts.rows.collect.foreach(println)
    println("\n")

    // // val A2 = coordFactorInit(N_dim,R)
    // val A2 = rowFactorInit(N_dim,R)
    // println(s"\nfactB: numRows=${A2.numRows}, numCols=${A2.numCols}")
    // println(s"A2 as IndexedRowMatrix")
    // // A2.toIndexedRowMatrix.rows.collect.foreach(println)
    // A2.rows.collect.foreach(println)

    // // val A3 = coordFactorInit(L_dim,R)
    // val A3 = rowFactorInit(L_dim,R)
    // println(s"\nfactC: numRows=${A3.numRows}, numCols=${A3.numCols}")
    // println(s"A3 as IndexedRowMatrix")
    // // A3.toIndexedRowMatrix.rows.collect.foreach(println)
    // A3.rows.collect.foreach(println)
    // println("\n")
    

    // ///////////////////////// for debugging ///////////////////////////////////
    // // C^T C, B^T B are the Garmian matricies! 
    // timer_start = Instant.now()
    // // var AtA_brz = A1.computeGramianMatrix()
    // var BtB_brz = A2.computeGramianMatrix()
    // var CtC_brz = A3.computeGramianMatrix()
    // // intermediate step - convert to Breeze DenseMatrix(BDM)
    // val M1M:BDM[Double] = new BDM[Double](CtC_brz.numRows,CtC_brz.numCols,CtC_brz.toArray)
    // val M2M:BDM[Double] = new BDM[Double](BtB_brz.numRows,BtB_brz.numCols,BtB_brz.toArray)
    // val tmp_breeze = M1M:*M2M
    // logger.info(s"Breeze hardamard matrix multiplication took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
    // println(s"tmp_breeze: rows=${tmp_breeze.rows}, cols=${tmp_breeze.cols}")
    // println(tmp_breeze)
    // println("\n")
    // timer_start = Instant.now()
    // val tmp_pinv = pinv(tmp_breeze)
    // logger.info(s"Breeze pseudo inverse took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
    // println(s"tmp_breeze: rows=${tmp_pinv.rows}, cols=${tmp_pinv.cols}")
    // println(tmp_pinv)

    // ///////////////////////////////////////////////////////////////////////////
    
    // /**  A^TA, B^TB, C^TC Hardamard products (element-wise multiplication)
    //   * 
    //   */
    // timer_start = Instant.now() 
    // val blkA3 = A3.toBlockMatrix
    // val blkA2 = A2.toBlockMatrix
    // // val blkA1 = A1.toBlockMatrix

    // timer_start = Instant.now() 
    // /////
    // val A3tA3 = (blkA3.transpose).multiply(blkA3).toIndexedRowMatrix
    // val A2tA2 = (blkA2.transpose).multiply(blkA2).toIndexedRowMatrix
    // // val AtA = (blkA1.transpose).multiply(blkA1).toIndexedRowMatrix

    // val A3hA2 = hmProduct(A3tA3,A2tA2)
    // /////
    // logger.info(s"Hardamard product A3hA2 with A3tA3/A2tA2 transposes took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
    // println(s"A3hA2: rows=${A3hA2.numRows}, cols=${A3hA2.numCols}")
    // A3hA2.entries.collect.foreach(println)
    // println("\n")

    // /** Moore-Penrose pseudo-inverse using Breeze LinAlg package
    //   *  - output is a Matrix, perfect for IndexedRowMatrix multiply!
    //   * 
    //   */ 
    // timer_start = Instant.now() 
    // /////
    // val A3hA2_pinv = mpPseudoInv(A3hA2.toIndexedRowMatrix) // returns a Matrix!
    // /////
    // logger.info(s"Moore-Pensrose A3hA2 pseudo inverse took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
    // println(s"A3hA2_pinv Pseudo Inverse\n$A3hA2_pinv")
    // println("\n")

    // /** Khatri-Rao Product (A3kA2=NLxR,CkA=NLxR,BkA=NNxR)
    //   * 
    //   */
    // timer_start = Instant.now() 
    // /////
    // val A3kA2 = krProduct(A3,A2)
    // /////
    // logger.info(s"Khatri-Rao product A3kA2 took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")

    // println(s"A3kA2")
    // A3kA2.rows.collect.foreach(println)
    // println("\n")

    // /** Multiply tensor, Khatri-Rao, and Hardamard 
    //   * 
    //   */
    // timer_start = Instant.now() 
    // /////
    // val Z_n = A3kA2.multiply(A3hA2_pinv)
    // val A1 = mode1Mat.toBlockMatrix.multiply(Z_n.toBlockMatrix)
    // /////
    // logger.info(s"Multiplying tensor,krProduct, and hmProduct took ${Duration.between(timer_start,Instant.now()).toMillis()} ms")
     
    // println(s"mode1")
    // mode1Mat.toIndexedRowMatrix.rows.collect.foreach(println)
    // println("\n")
    // println(s"Z_n")
    // Z_n.rows.collect.foreach(println)
    // println("\n")

    // println(s"A1")
    // A1.toIndexedRowMatrix.rows.collect.foreach(println)
    // println("\n")
   
    // sys.exit()

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
    // Run PageRank for a fixed number of iterationsions.
    
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