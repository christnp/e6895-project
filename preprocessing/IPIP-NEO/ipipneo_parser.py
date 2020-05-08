# import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import time
import csv

from random import sample 

import pycountry 

# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,udf,struct,year,collect_list,asc
from pyspark.sql.types import *

# from google.cloud import logging
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import google.auth
from google.oauth2 import service_account

MODE = "RELEASE" #"DEBUG"     # debug uses local directory, release uses GCP bucket
SMALL_DATA = False # use a subset of the 'final' dataset

# GCP globals
GCP_BUCKET = "eecs-e6895-bucket"
GCP_PROJECT = "eecs-e6895-edu"
# BQ_DATASET = "emu_dataset"
# BQ_TABLE = "emudata_2" 

 # setup GCP creds (default uses local GOOGLE_APPLICATION_CREDENTIALS)
# credentials, project = google.auth.default(
#     scopes=['https://www.googleapis.com/auth/cloud-platform',
#             'https://www.googleapis.com/auth/logging.admin'])
# credentials = service_account.Credentials.from_service_account_file(
#         filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
#         scopes=["https://www.googleapis.com/auth/cloud-platform", \
#          "https://www.googleapis.com/auth/logging.admin"]
#     )

# simple helpers to making logging more fluid
# Severity Levels:
DEFAULT     = "DEFAULT"     # (0) The log entry has no assigned severity level.
DEBUG       = "DEBUG"       # (100) Debug or trace information.
INFO        = "INFO"        # (200) Routine information, such as ongoing status or performance.
NOTICE      = "NOTICE"      # (300) Normal but significant events, such as start up, shut down, or a configuration change.
WARNING     = "WARNING"     # (400) Warning events might cause problems.
ERROR       = "ERROR"       # (500) Error events are likely to cause problems.
CRITICAL    = "CRITICAL"    # (600) Critical events cause more severe problems or outages.
ALERT       = "ALERT"       # (700) A person must take an action immediately.
EMERGENCY   = "EMERGENCY"   # (800) One or more systems are unusable.

def get_log_entries(logger):
    """
    """

    all_entries = logger.list_entries(page_size=10)
    entries = next(all_entries.pages)

    for entry in entries:
        timestamp = entry.timestamp.isoformat()
        print('* {}: {}'.format
              (timestamp, entry.payload))

    return 'Done!'


def main(): 

    ############################################################################
    # TODO: Google StackDriver logging => 
    # https://medium.com/dunnhumby-data-science-engineering/how-to-get-more-value-out-of-your-application-logs-in-stackdriver-c6881de1bece
    # https://googleapis.dev/python/logging/latest/usage.html#delete-all-entries-for-a-logger
    # https://google-cloud-python.readthedocs.io/en/0.32.0/logging/usage.html#export-to-cloud-storage
    # Instantiates a client and connects logger to root logging handler; by 
    # default this captures all logs at INFO level and higher
    # client = google.cloud.logging.Client()                    
    # client.setup_logging() # suppposed to connect to stackdriver

    # Google API direct
    log_name = 'ipipneo-parser2' 
    logging_client = google.cloud.logging.Client()
    logger = logging_client.logger(log_name) # for usign google API calls
    # logger.log_text('Google Logging 3',severity=ERROR) 

     # python logging integration
    logging.basicConfig(format="%(asctime)s %(levelname)s >>>> %(message)s", datefmt='%y/%m/%d %H:%M:%S')
    logging_client.setup_logging(log_level=logging.INFO) # excluded_loggers=("werkzeug",))
    handler = CloudLoggingHandler(logging_client, name=log_name) # assigns to google logger = name
    cloud_logger = logging.getLogger(log_name) # gives the python logger the name (" ")
    # cloud_logger.setLevel(logging.INFO)
    cloud_logger.addHandler(handler)
    cloud_logger.info("Starting application...")

    ############################################################################

    if MODE == DEBUG:
        basepath = "/home/christnp/Development/e6895/data"
        # raw IPIP data location (must be csv)
        if SMALL_DATA:
            raw_ipip = os.path.join(basepath,"IPIP-NEO/release/IPIP120-1000.dat")
        else:
            raw_ipip = os.path.join(basepath,"IPIP-NEO/release/IPIP120.dat")
        new_ipip = os.path.join(basepath,"IPIP-NEO/release/IPIP120-final.csv")
        raw_parkey = os.path.join(basepath,"IPIP-NEO/release/IPIP-NEO-ParseKey.csv")
        raw_names = os.path.join(basepath,"IPIP-NEO/release/us_names.csv")
    else:
        basepath = "gs://{}".format(GCP_BUCKET)
        if SMALL_DATA:
            raw_ipip = os.path.join(basepath,"input/IPIP120-1000.dat")
        else:
            raw_ipip = os.path.join(basepath,"input/IPIP120.dat")
        new_ipip = os.path.join(basepath,"output/IPIP120-final.csv")
        raw_parkey = os.path.join(basepath,"input/IPIP-NEO-ParseKey.csv")
        raw_names = os.path.join(basepath,"input/us_names.csv")

    # begin spark application
    spark = SparkSession \
            .builder \
            .appName("IPIP-NEO-120 Full") \
            .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    # load the data file
    ipip_data = sc.textFile(raw_ipip)
    cloud_logger.info("Opened {}".format(raw_ipip))

    ipip_parkey = sc.textFile(raw_parkey)
    cloud_logger.info("Opened {}".format(raw_parkey))


    name_data = sc.textFile(raw_names)
    cloud_logger.info("Opened {}".format(raw_names))

   

    ############################################################################
    # Parser key
    #filter out the header and parse the variables
    cloud_logger.info("Building parser key...")
    parkey_header = ipip_parkey.first()
    ipip_parkey = ipip_parkey.filter(lambda line: line != parkey_header) \
                            .map(lambda line: line.split(",")).collect()
    # set the column name and schema variables
    columns = [x[0] for x in ipip_parkey]
    schema = {x[0]: x[3] for x in ipip_parkey}

    ############################################################################
    # Name data
    cloud_logger.info("Building name data...")
    name_header = name_data.first()
    name_data = name_data.filter(lambda line: line != name_header) \
                            .map(lambda line: line.split(",")).toDF(["NAME","SEX","YEAR"]) \
                            .repartition(24*4).cache() # #CPU*4
                            # .repartition("YEAR").cache()
    
    #########################################################################################
    # parse each line of the datafile
    def parseLine(line,parkey):
        cloud_logger.info("Starting parseLine() function...")

        # column,varbeg,varend,vartyp = parkey
        # console = ""
        # var_str = []
        # date_dict = {}
        parsed = {}
        for column,varbeg,varend,vartyp in parkey:            
            # parse variable'bit out of the line string
            var_bit = line[int(varbeg)-1:int(varend)]

            ###########################
            # convert days from 1900 to correct year
            # if column in ["YEAR","MONTH","DAY","HOUR","MIN","SEC"]:   
            if column == "DATE":                  
                # SEC	10	11
                # MIN	12	13	float
                # HOUR	14	15	float
                # DAY	16	17	float
                # MONTH	18	19	float
                # YEAR	20	22	float
                
                ss = int(var_bit[0:2])
                mm = int(var_bit[2:4])
                hh = int(var_bit[4:6])
                d = int(var_bit[6:8])
                m = int(var_bit[8:10])
                y = int(var_bit[10:13])+1900
                try:
                    var_bit = datetime(y,m,d,hh,mm,ss)
                except Exception as e:
                    e_str = str(e).lower()
                    if(e_str.find('hour must be in 0..23') >= 0):
                        ss = ss%59
                    elif(e_str.find('minute must be in 0..59') >= 0):
                        mm = mm%59
                    elif(e_str.find('hour must be in 0..23') >= 0):
                        hh = hh%23
                    elif(e_str.find('day is out of range for month') >= 0): 
                        d = 1
                        m = m + 1
                    elif(e_str.find('month must be in 1..12') >= 0):
                        d = 1
                        m = 1
                    elif(e_str.find('year {} is out of range'.format(str(y))) >= 0):
                        y = 1900
                    else:
                        print("\n\nError: {}\n >> var_bit: {}".format(e,var_bit))
                        ss = mm = hh = 0
                        d = m = 1
                        y = 1900
                        cloud_logger.warning("Datetime not parsed; {}".format(var_bit))
                    # try it again
                    try:
                        var_bit = datetime(y,m,d,hh,mm,ss)
                    except Exception as e:
                        var_bit = "NaT"
                        cloud_logger.warning("Datetime not converted; {}".format(var_bit))

                # dt_str = var_bit.strftime("%Y-%m-%d %H:%M:%S")
                # print(dt_str)
                
            ###########################
            # convert 1 = Male, 2 = Female to "M" or "F"
            elif column == "SEX":
                if var_bit == '1':
                    var_bit = "M"
                elif var_bit == '2':
                    var_bit = "F"
                else:              
                    cloud_logger.warning("Sex not converted; {}".format(var_bit))
                    var_bit = "None"
            ########################## 
            # convert country to correcy short code
            elif column == 'COUNTRY':
                country_str = str(var_bit).lower().strip()
                try:
                    var_bit = pycountry.countries.search_fuzzy(country_str)[0].alpha_3
                except Exception as e: # some known country codes that will error
                    if country_str == "south kor":
                        var_bit = pycountry.countries.search_fuzzy("Korea, Republic of")[0].alpha_3                
                    elif country_str == "st lucia":
                        var_bit = pycountry.countries.search_fuzzy("Saint Lucia")[0].alpha_3
                    else:
                        cloud_logger.warning("Country not converted; {}".format(var_bit))
                        var_bit = "ZZZ" # unknown
            ########################## 
            # all else, just strip() whitespace
            else:
                var_bit = var_bit.strip()
            # store as dictionary            
            parsed[column] = var_bit
        
        cloud_logger.info("Finshed parseLine() function...")        
        return [parsed[k] for k in parsed] #list(parsed.values)


    #########################################################################################
    cloud_logger.info("READING DATA INTO RDD")       
    # lines = ipip_data.flatMap(lambda line: parseLine(line,ipip_parkey))
    lines = ipip_data.map(lambda line: parseLine(line,ipip_parkey))
    
    # print("\n-------- READING DATA INTO RDD -------- \n")
    # lines = ipip_data.map(lambda line: \
    #             [line[int(x[1])-1:int(x[2])].strip() for x in ipip_parkey])

    #########################################################################################
    cloud_logger.info("READING RDD INTO DATAFRAME (DF)")       
    # turn RDD into a dataframe (column values from parsing key) 
    # number_of_partitions = number_of_cpus * 4
    ipipDF = lines.toDF(columns)
    cloud_logger.info("initial ipipDF.rdd.getNumPartitions() = {}".format(ipipDF.rdd.getNumPartitions()))       
    # apply the schema
    ipipDF = ipipDF.select([col(c).cast(schema[c]) for c in ipipDF.columns]).repartition(24*4).cache()
    cloud_logger.info("ipipDF.rdd.getNumPartitions() = {}".format(ipipDF.rdd.getNumPartitions()))       
    
    ############################################################################
    # select name from US Names database (match SEX and BIRTH YEAR, then randomly
    # select form name list)
    cloud_logger.info("GENERATE NAME & UPDATE DF")       
    # temporay DF with birthyear, repartition based on birthyear
    
    cloud_logger.info("Start withColumn() and repartition()...")  

    tmpDF = ipipDF.withColumn('BIRTH_YEAR',year(col('DATE'))-col('AGE')).repartition(24*4) # #CPU*4 .repartition("BIRTH_YEAR")
    cloud_logger.info("tmpDF.rdd.getNumPartitions() = {}".format(tmpDF.rdd.getNumPartitions()))       
    

    cloud_logger.info("Start join() and drop dummy columns...")       
    tmpDF = tmpDF.join(name_data, (name_data['YEAR'].alias('_YEAR') == tmpDF['BIRTH_YEAR']) \
                        & (name_data['SEX'].alias('_SEX') == tmpDF['SEX']),"inner") \
                        .drop('_SEX').drop('_YEAR').drop('BIRTH_YEAR').repartition(24*4) # #CPU*4
    cloud_logger.info("tmpDF.rdd.getNumPartitions() = {}".format(tmpDF.rdd.getNumPartitions()))       

    cloud_logger.info("Start groupBy() with Agg()...")       
    nameDF = tmpDF.groupBy('CASE').agg(collect_list('NAME')) \
                    .rdd.map(lambda row: (row[0],sample(row[1],1)[0])) \
                    .toDF(['CASE','NAME']).repartition(24*4) # #CPU*4
    cloud_logger.info("df.rdd.getNumPartitions() = {}".format(nameDF.rdd.getNumPartitions()))       

    # nameDF = nameDF.alias('a').join(ipipDF.alias('b'),col('b.CASE') == col('a.CASE')) \
    #             .select([col('a.'+xx) for xx in nameDF.columns] + [col('b.'+xx) for xx in ipipDF.columns if col('b.'+xx) not in {'CASE'}])
    cloud_logger.info("Start join() of nameDF with ipipDF...")       
    finalDF = nameDF.join(ipipDF,['CASE'])
    cloud_logger.info("df.rdd.getNumPartitions() = {}".format(finalDF.rdd.getNumPartitions()))       
    

    # save the dataframe to a single file (use repartition to ensure HEAP mem adjusts)
    cloud_logger.info("Start repartition(1) of final dataset...")       
    finalDF.repartition(1).sortWithinPartitions(asc("CASE")) \
            .write.csv(new_ipip,mode='overwrite',header='true')
    
    # google logger stuff
    get_log_entries(logger)

if __name__ == "__main__": 
  
    # calling main function 
    main() 