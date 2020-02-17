# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 21:47:32 2020

@author: vegah
"""
import logging
import configparser
from pyspark.sql import SparkSession, SQLContext
import Utils

class Gdelt_themes_ingester:
    """
    Handles GDELT graph data file
    """

    def __init__(self):
        """
        Setting up Spark session and Spark context, AWS access key
        """
        logging.basicConfig(filename='gdelt.log', format='%(asctime)s %(levelname)s: %(message)s ', level=logging.INFO)
        logging.info('STARTING Gdelt_themes_ingester')

        config = configparser.ConfigParser()
        config.read('../../config/gdelt.ini')
                
        self.dbParams =  {'url' : config['PostgreSQL']['url']
                     ,'dbtable_themes' : config['PostgreSQL']['dbtable_themes']
                     ,'user' : config['PostgreSQL']['user']
                     ,'password' : config['PostgreSQL']['password']
                     ,'stringtype' : config['PostgreSQL']['stringtype']
                     ,'driver' : config['PostgreSQL']['driver']
                     }
        
        spark = SparkSession.builder \
            .appName('bubble-breaker') \
            .getOrCreate()
        SparkSession.sparkContext.setLogLevel("WARN")
    
        self.sc=spark.sparkContext
        
    def main(self):
        """
        Extracts persons from csv file to store in persons table
        """
        logging.info('Started main()')
        sqlContext = SQLContext(self.sc)
        
        logging.info('Starting themes ingester.')
        themesRDD = self.sc.textFile(
            '/home/ubuntu/gitRoot/data/GDELT-Global_Knowledge_Graph_CategoryList.csv') 
        themesRDD = themesRDD.map(lambda x: x.encode('utf', 'ignore'))
        themesRDD.cache()
        themesRDD = themesRDD.map(lambda x: x.split('\t'))

        themesDF     = sqlContext.createDataFrame(themesRDD)

        themesDF = themesDF.withColumnRenamed("_1", "theme_type") \
            .withColumnRenamed("_2","theme") \
            .withColumnRenamed("_5","description") \
            .select('theme_type','theme','description')

        logging.info(themesDF.take(10))

        Utils.save_to_db(themesDF, 'dbtable_themes')

########################################################3        
if __name__ == '__main__':
    ggfi = Gdelt_themes_ingester()
    ggfi.main()