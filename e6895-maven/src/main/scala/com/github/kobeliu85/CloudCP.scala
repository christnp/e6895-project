/**
  * Created by cqwcy201101 on 12/6/16.
  * https://github.com/kobeliu85/Spark-Tensor
  * 
  * Modified by christnp on 5/9/20  
  */
// package org.apache.spark.mllib.linalg.distributed.CloudCP
package com.github.kobeliu85

import breeze.linalg.{max, pinv, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{abs, sqrt}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrices, Matrix}


object CloudCP {

  def readFile(s:RDD[String]):RDD[Vector] ={
    val RDD_V = s.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    RDD_V

  }

  def BDMtoMatrix(InputData:BDM[Double]):Matrix={
    val M: Matrix = Matrices.dense(InputData.rows, InputData.cols, InputData.data)

    M
  }

  def BDVtoVector(InputData:BDV[Double]):Vector={
    val V: Vector = Vectors.dense(InputData.toArray)

    V
  }

  // NC: added 09 May 2020
  def IndexedRowMatrixToRDD_V(rmat:IndexedRowMatrix):RDD[Vector] ={
    val RDD_V:RDD[Vector] = rmat.rows.map{case IndexedRow(i,r) => r}

    RDD_V    
  }

  def RDD_VtoRowMatrix(RDDdata:RDD[Vector]):RowMatrix = {
    val Result:RowMatrix = new RowMatrix(RDDdata)
    Result

  }

  def InitialRowMatrix(Size:Long,
                       Rank:Int,
                       sc:SparkContext):(RowMatrix)={

    val rowData = RandomRDDs.uniformVectorRDD(sc,Size,Rank)
      .map(x => Vectors.dense(BDV.rand[Double](Rank).toArray))
    val matrixRandom: RowMatrix = new RowMatrix(rowData,Size,Rank)

    matrixRandom
  }


  def InitialIndexedRowMatrix(Size:Long,
                              Rank:Int,
                              sc:SparkContext)={

    val tempRowMatrix: RowMatrix = InitialRowMatrix(Size,Rank,sc)
    val map = tempRowMatrix.rows.zipWithIndex()
      .map{case (x,y) => IndexedRow(y, Vectors.dense(x.toArray))}
    val Result:IndexedRowMatrix = new IndexedRowMatrix (map)

    Result
  }


  def Compute_MTM_RowMatrix(matrix:IndexedRowMatrix)={
    val mTm = matrix.computeGramianMatrix()
    val MTM:BDM[Double] = new BDM[Double](mTm.numRows, mTm.numCols, mTm.toArray)

    MTM
  }

  def GenM1(SizeOfMatrix:Long,
            Rank:Int,
            sc:SparkContext):IndexedRowMatrix = {

    val M1:IndexedRowMatrix =
      new IndexedRowMatrix(InitialIndexedRowMatrix(SizeOfMatrix,Rank,sc)
        .rows.map(x =>
        IndexedRow(x.index,Vectors.zeros(Rank))))

    M1
  }

  def K_Product(v:Double,
                DV_1:BDV[Double],
                DV_2:BDV[Double]):BDV[Double] = {

    val Result:BDV[Double] = (DV_1:*DV_2) :*= v

    Result
  }

  def CalculateM2(m1:IndexedRowMatrix,
                  m2:IndexedRowMatrix):Matrix = {

    val M1M = Compute_MTM_RowMatrix(m1)
    val M2M = Compute_MTM_RowMatrix(m2)

    val result:Matrix = BDMtoMatrix(pinv(M1M :* M2M))

    result

  }

  def CalculateM1(TensorDta:RDD[Vector],
                  m1:IndexedRowMatrix,
                  m2:IndexedRowMatrix,
                  Dim:Int,
                  SizeOfMatrix:Long,
                  Rank:Int,
                  sc:SparkContext):IndexedRowMatrix = {

    val index_1:Int = (Dim+1)%3; // index_1 (Dim=0=>j, Dim=1=>k, Dim=2=>i);
    val index_2:Int = (Dim+2)%3

    val InitialM1:IndexedRowMatrix = GenM1(SizeOfMatrix,Rank,sc)
    println("\nCalculateM1 (top)")
    println("-----------------")
    
    println("InitialM1")
    InitialM1.rows.collect.foreach(println)

    val ReduceResult = TensorDta.map(
      x => (x.apply(index_1).toLong, x))
      .join(m1.rows.map(x => (x.index, x)))
      .values.map (x => (x._1.apply(index_2).toLong, x))
      .join(m2.rows.map(x => (x.index, x)))
      .mapValues(x => (x._1._1, x._1._2, x._2))
      .values.map(x => (x._1, K_Product(x._1.apply(3), BDV[Double](x._2.vector.toArray),BDV[Double](x._3.vector.toArray))))
      .map(x => (x._1.apply(Dim).toLong, x._2))
      .reduceByKey((x,y) => x+y)
    //.sortByKey()

    println("ReduceResult")
    ReduceResult.collect.foreach(println)

    //ReduceResult.collect().foreach(println)

    val tempM1 = InitialM1.rows.map(
      x => (x.index,BDV[Double](x.vector.toArray)))
      .cogroup(ReduceResult)
      .mapValues{x =>
        if (x._2.isEmpty) {
          BDVtoVector(x._1.head)}
        else {
          BDVtoVector(x._2.head)}
      }//.sortByKey()

      // println("tempM1")
      // tempM1.collect.foreach(println)

    val ResultM1:IndexedRowMatrix = new IndexedRowMatrix(
      tempM1.map(
        x =>IndexedRow(x._1,Vectors.dense(x._2.toArray))))
      
      println("\nCalculateM1 (end)")
      println("-----------------")
      println("ResultM1")
      ResultM1.rows.collect.foreach(println)

    ResultM1
  }


  def ComputeFit(TensorData:RDD[Vector],
                 L:BDV[Double],
                 A:IndexedRowMatrix,
                 B:IndexedRowMatrix,
                 C:IndexedRowMatrix,
                 ATA:BDM[Double],
                 BTB:BDM[Double],
                 CTC:BDM[Double]) = {

    val tmp:BDM[Double] = (L*L.t) :* ATA :*BTB :* CTC
    val normXest = abs(sum(tmp))
    val norm = TensorData.map(x => x.apply(3)*x.apply(3)).reduce(_+_)

    println("\nComputeFit (top)")
    println("-----------------")

    println(">> L & (L*L.t)")
    println(L)
    println(L*L.t)
    
    println("tmp")
    println(tmp)
    println("normXest")
    println(normXest)
    println("norm")
    println(norm)
    println("TensorData")
    TensorData.collect.foreach(println)

    var product = 0.0
    val Result = TensorData.map(
      x => (x.apply(0).toLong, x))
      .join(A.rows.map(x => (x.index, x)))
      .values.map(x => (x._1.apply(1).toLong, x))
      .join(B.rows.map(x => (x.index, x)))
      .values.map(x => (x._1._1.apply(2).toLong, x))
      .join(C.rows.map(x => (x.index, x)))
      .mapValues(x => (x._1._1._1,
        BDV[Double](x._1._1._2.vector.toArray),
        BDV[Double](x._1._2.vector.toArray),
        BDV[Double](x._2.vector.toArray)))
      .values.map(x => K_Product(x._1.apply(3),x._4,K_Product(1.0,x._2,x._3)))
      .reduce(_+_)
    
    println("Result")
    println(Result)
    println("Result.t")
    println(Result.t)

    product = product + Result.t * L
    val residue = sqrt(normXest + norm - 2*product)
    val Fit = 1.0 - residue/sqrt(norm)
    
    println("residue")
    println(residue)

    Fit
  }

  // NC: added 09 May 2020 to avoid Spark.SQL abs() conflict
  def FitAbs(fit : Double) : Double = {
      val AbsFit = abs(fit)

      AbsFit
  }

  def UpdateLambda(matrix:IndexedRowMatrix,
                   N:Int) = {

    if (N == 0){
      val L:BDV[Double] = BDV[Double](matrix.toRowMatrix()
        .computeColumnSummaryStatistics().normL2.toArray)

      L
    }
    else {
      val L:BDV[Double] = BDV[Double](matrix.toRowMatrix()
        .computeColumnSummaryStatistics().max.toArray)
        .map(x=> max(x,1.0))

      L
    }
  }


  def UpdateMatrix(TensorData:RDD[Vector],
                   m1:IndexedRowMatrix,
                   m2:IndexedRowMatrix,
                   Dim:Int,
                   SizeOfMatrix:Long,
                   Rank:Int,
                   sc:SparkContext):IndexedRowMatrix = {
    
    println("\nUpdateMatrix (top)")
    println("-----------------")
    println("TensorData")
    TensorData.collect.foreach(println)

    // println("m1")
    // m1.rows.collect.foreach(println)
    // println("m2")
    // m2.rows.collect.foreach(println)
    // println("Dim")
    // println(Dim)
    // println("SizeOfMatrix")
    // println(SizeOfMatrix)
    // println("Rank")
    // println(Rank)

    val updateM = CalculateM1(TensorData,m1,m2,Dim,SizeOfMatrix,Rank,sc)
      .multiply(CalculateM2(m1,m2))
    
    println("\nUpdateMatrix (end)")
    println("-----------------")
    println("updateM")
    updateM.rows.collect.foreach(println)

    updateM
  }

  // NC: updated 5/9/20 to remove NaNs (.map(y => if (y.isNaN()) 0.0 else y))
  def NormalizeMatrix(matrix:IndexedRowMatrix,
                   L:BDV[Double]) ={

    val map_M = matrix.rows
      .map(x => (x.index, BDV[Double](x.vector.toArray)))
      .mapValues(x => (x:/L).map(y => if (y.isNaN()) 0.0 else y))

    val NL_M:IndexedRowMatrix = new IndexedRowMatrix(map_M
      .map(x => IndexedRow(x._1, Vectors.dense(x._2.toArray))))

    NL_M
  }


  def OutputResult(Matrix:IndexedRowMatrix,
                   path:String) = {

    Matrix.rows.sortBy(x => x.index).saveAsTextFile(path)

  }



}
