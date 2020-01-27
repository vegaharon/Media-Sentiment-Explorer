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
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, collect_list, split
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType, BooleanType, DataType
import logging

import utils as f
import configparser


def ingest_persons(sc, dbParams):
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
    gkgRDD = sc.textFile('/home/ubuntu/gitRoot/data/gdelt/20190701000000.gkg.csv') #for EC2 testing
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    # gkgRDD = gkgRDD.filter(lambda x: len(x)==27)
    gkgRDD = gkgRDD.filter(lambda x: f.is_not_empty([x[11]]))
    gkgRowRDD = gkgRDD.map(lambda x : Row(name = x[11].split(';')[:-1]))
    
    sqlContext = SQLContext(sc)
    #Transform RDDs to dataframes
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)
    gkgDF.show(10)
    logging.info("populated DF, persons: %s", gkgDF.show(10))
    logging.info(gkgDF.show(10))

    #Insert in the persons table
    #MUST move the credentials to a config file and not submit to git
    gkgDF.write.format("jdbc") \
    .option("url", dbParams("url")) \
    .option("dbtable", dbParams("dbtable")) \
    .option("user", dbParams("user")) \
    .option("password", dbParams("password")) \
    .option("stringtype", dbParams("stringtype")) \
    .option("driver", dbParams("driver")) \
    .mode('append').save()

def main(sc, dbParams):
    """
    Read GDELT data from S3, split and process words in themes,
    and perform word count. Taxonomy words are defined as word
    count >= occurrence_cut. List of taxonomy words written to
    out_file_name
    """
    logging.basicConfig(filename="gdelt.log", format='%(asctime)s %(levelname)s: %(message)s ', level=logging.INFO)
    logging.info("started main()")

    ingest_persons(sc, dbParams)


if __name__ == '__main__':
    
    """
    Setting up Spark session and Spark context, AWS access key
    """
    config = configparser.ConfigParser()
    config.read('../../config/gdelt.ini')
            
    dbParams =  {"url" : config['PostgreSQL']['url']
                 ,"dbtable" : config['PostgreSQL']['dbtable']
                 ,"user" : config['PostgreSQL']['user']
                 ,"password" : config['PostgreSQL']['password']
                 ,"driver" : config['PostgreSQL']['driver']
                 }
    
    spark = SparkSession.builder \
        .appName("bubble-breaker") \
        .getOrCreate()

    sc=spark.sparkContext

    main(sc, dbParams)    
    