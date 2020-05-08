/** Commonly used Apache Spark functions.
  *
  * This is a package for all the commonly used Spark functions
  */
package edu.columbia.advbigdata.utils

// spark-core_2.11 artifacts
import org.apache.spark.rdd.RDD

// spark-sql_2.11 artifacts
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.{DataFrame,Row,Column}

// spark-bigquery_2.11 artifacts
import com.google.cloud.spark.bigquery._



/**
 * Factory for package [[utils.sparkutils]] 
 * 
 */
package object sparkutils {  

    /**
    * Implicit methods class that extend Spark Dataframe, Google BQ, etc.
    */
    implicit class SparkExtensions(df : DataFrame) {  

        /** Prints the goals dataframe (but really, any dataframe)
        *
        *  @param cols list of column name strings to include in output
        *  @return prints each row as a list
        */
        def printGoals(cols: Seq[String]) = {
            df.select(cols.head,cols.tail:_*).foreach { row => 
            println(row.toSeq)
            }

        }
        /** Prints the goals dataframe (specifically)
        *
        *  @param none
        *  @return prints each row of DF, including only the uuid and goals
        */
        def printGoals() = {
        // df.foreach { row => row.toSeq.foreach{goal => println(goal) }
            val cols = Seq("uuid","goals")
            val goals = df.select(cols.head,cols.tail:_*).collect.toList
            // print results
            goals.foreach { tup=> {
                println(tup(0) + ": Goals")
                print(" >> " + tup(1))
                println("\n")
            }
            }
        }
         /**
         * Helper function to write CSV file as a single file. This function will
         * attempt to us spark.dataFrame.coalesce() to pull all the distributed
         * data together such that a single file can be written.
         *
         *  @param csvFile string representing the path to the source CSV file
         *  @param del user specified delimiter (not used)
         *  @return no return
         */
        def writeToCSV(csvFile: String, del: String = ",") : String =
        {
            // get the SparkSession from the DataFrame
            val spark =  df.sparkSession
            try {
                // write it to a single (coalesce) CSV
                df.coalesce(1).write.format("csv")
                    .option("header", "true")
                    .mode("overwrite")
                    .save(csvFile)
                return csvFile
            } 
            catch 
            {        
                case unknown:  Throwable => {
                    println("Failed to write CSV '" + csvFile + "' \n  at " + unknown) 
                    return ""  
                }
            }
        }
        /**
         * Helper function to read CSV file into Spark DataFrame. This function will
         * attempt to parse the file for the user defined delimeter first and, if that
         * fails, will attempt to parse using standard comma-delimiter.
         *
         *  @param csvFile string representing the path to the source CSV file
         *  @param del user specified delimiter
         *  @return a Spark DataFrame with raw data
         */
        def readFromCSV(csvFile: String, del: String = ",") : DataFrame =
        {
            // get the SparkSession from the DataFrame
            val spark =  df.sparkSession
            // read data from the CSV file
            var csvDF = spark.emptyDataFrame
            try {
                csvDF = spark.read.format("csv")
                    .option("delimiter", del)
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(csvFile)
            } catch {        
                case unknown:  Throwable => {
                    println("Failed to parse CSV with '" + del + "' \n  at " + unknown)   
                    println("\nAttempting to parse CSV again using ',' ")
                    try
                    {
                        csvDF = spark.read.format("csv")
                                .option("delimiter", ",")
                                .option("header", "true")
                                .option("inferSchema", "true")
                                .load(csvFile)
                    } 
                    catch 
                    {
                        case unknown:  Throwable => println("Failed to parse CSV with ','\n  at " + unknown)
                        println("\n>> Closing SparkSession [" + spark.sparkContext.applicationId + "]\n")
                        spark.stop()
                        // System.exit(1)
                        throw new Exception("Failed to parse CSV file, please verify file input: " + csvFile);
                    }
                }
            }
            return csvDF
        }

        /** Writes data to Google BQ table
        *
        *  @param bucket string name of the  GCP bucket for temp storage
        *  @param table string name of the GCP BQ dataset.table to store the data
        *  @return prints each row of DF, including only the uuid and goals
        */
        def writeToBQ(bucket: String,table: String = "temp",mode: SaveMode = SaveMode.Append,exit: Boolean = true) = {
            // get the SparkSession from the DataFrame
            val spark =  df.sparkSession
            // Saving the data to BigQuery.
            try
            {
                df.write.format("bigquery")
                    .option("table",table)
                    .option("temporaryGcsBucket",bucket)
                    .mode(mode)
                    // .option("credentials", gcp_cred)
                    .save()
            } 
            catch 
            {
                case unknown:  Throwable => println("Failed to save data to BQ table " + table + " ','\n  at " + unknown)
                // if "exit", then close session and crash; else, fails gracefully
                if(exit)
                {
                    println("\n>> Closing SparkSession [" + spark.sparkContext.applicationId + "]\n")
                    spark.stop()
                    throw new Exception("Not sure what to do... ");
                }
            }
        }

        /** Reads data from Google BQ table
        *
        *  @param bucket string name of the  GCP bucket for temp storage
        *  @param table string name of the GCP BQ dataset.table to read data from
        *  @return prints each row of DF, including only the uuid and goals
        */
        def readFromBQ(bucket: String,table: String = "temp",exit: Boolean = true) : DataFrame = {
            // get the SparkSession from the DataFrame
            val spark =  df.sparkSession
            // read the data from BigQuery.
            var bqDF = spark.emptyDataFrame
            try
            {
                bqDF = spark.read.format("bigquery")
                    .option("table",table)
                    .load()
            } 
            catch 
            {
                case unknown:  Throwable => println("Failed to load data from BQ table " + table + " ','\n  at " + unknown)
                // if "exit", then close session and crash; else, fails gracefully
                if(exit)
                {
                    println("\n>> Closing SparkSession [" + spark.sparkContext.applicationId + "]\n")
                    spark.stop()
                    throw new Exception("Not sure what to do... ");
                }
            }

            return bqDF
        }
    }
}