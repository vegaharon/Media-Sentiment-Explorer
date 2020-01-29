# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 17:09:27 2020

@author: Vegan Aharonian
"""

import sys
import os

from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, \
                                  collect_list, split
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType, \
                                 BooleanType, DataType
import logging

import utils as f
import configparser

class gdelt_gkg_file_ingester:
    
    #dbParams =  {}
    
    def ingest_persons(self):
        """
        Extracts persons from csv file to store in persons table
        Parameters
        ----------
        sc : TYPE
            DESCRIPTION.
    
        Returns
        -------
        None.
    
        """
        
        logging.info("Started ingest_person")
        #Read 'GKG" table from GDELT S3 bucket. Transform into RDD
        # gkgRDD = sc.textFile('s4a://gdelt-open-data/v2/gkg/2018*.gkg.csv')
        gkgRDD = self.sc.textFile(
            '/home/ubuntu/gitRoot/data/gdelt/20190701000000.gkg.csv') 
        gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
        gkgRDD.cache()
        gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
        # gkgRDD = gkgRDD.filter(lambda x: len(x)==27)
        gkgRDD = gkgRDD.filter(lambda x: f.is_not_empty([x[11]]))
        gkgRowRDD = gkgRDD.map(lambda x : Row(name = x[11].split(';')[:-1],
                                              theme = x[7].split(';')[:-1]))
        
        sqlContext = SQLContext(self.sc)
        #Transform RDDs to dataframes
        gkgDF     = sqlContext.createDataFrame(gkgRowRDD)
        explodedDF = gkgDF.select(explode(gkgDF.name).alias("name")).distinct()
        
        logging.info("Starting to insert into persons table.")
    
        #Insert in the persons table
        #MUST move the credentials to a config file and not submit to git
        explodedDF.write.format("jdbc") \
        .option("url", self.dbParams["url"]) \
        .option("dbtable", self.dbParams["dbtable"]) \
        .option("user", self.dbParams["user"]) \
        .option("password", self.dbParams["password"]) \
        .option("stringtype", self.dbParams["stringtype"]) \
        .option("driver", self.dbParams["driver"]) \
        .mode('append').save()
        
        def main():
            """
            Read GDELT data from S3, split and process words in themes,
            and perform word count. Taxonomy words are defined as word
            count >= occurrence_cut. List of taxonomy words written to
            out_file_name
            """
            
            logging.basicConfig(filename="gdelt.log", format='%(asctime)s %(levelname)s: %(message)s ', level=logging.INFO)
            logging.info("started main()")

    
    def __init__(self):
    #
        
        """
        Setting up Spark session and Spark context, AWS access key
        """
        config = configparser.ConfigParser()
        config.read('../../config/gdelt.ini')
                
        self.dbParams =  {"url" : config['PostgreSQL']['url']
                     ,"dbtable" : config['PostgreSQL']['dbtable']
                     ,"user" : config['PostgreSQL']['user']
                     ,"password" : config['PostgreSQL']['password']
                     ,"stringtype" : config['PostgreSQL']['stringtype']
                     ,"driver" : config['PostgreSQL']['driver']
                     }
        
        spark = SparkSession.builder \
            .appName("bubble-breaker") \
            .getOrCreate()
    
        self.sc=spark.sparkContext
        logging.basicConfig(filename="gdelt.log", format='%(asctime)s %(levelname)s: %(message)s ', level=logging.INFO)
        logging.info("started main()")

########################################################3        

    
ggfi = gdelt_gkg_file_ingester()
#ggfi.main()
ggfi.ingest_persons()