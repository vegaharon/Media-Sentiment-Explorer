# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 17:09:27 2020

@author: Vegan Aharonian
"""

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, split
import logging
import Utils

class Gdelt_gkg_file_ingester_elections:
    """
    Ingests GDELT graph data file. Takes a slice related to presidential elections. 
    """

    def __init__(self):
        """
        Setting up Spark session and Spark context, AWS access key
        """
        logging.basicConfig(filename='gdelt.log', format='%(asctime)s %(levelname)s: %(message)s ', level=logging.INFO)
        logging.info('STARTING Gdelt_gkg_file_ingester')
        
        spark = SparkSession.builder \
            .appName('bubble-breaker') \
            .getOrCreate()
    
        self.sc=spark.sparkContext
        self.sc.setLogLevel("WARN")
        
 
    def build_elections_sentiment_explorer(self, gkgDF):        
        gkgDF = gkgDF \
            .withColumnRenamed("_1","rec_id") \
            .withColumnRenamed("_2","file_date") \
            .withColumnRenamed("_8","theme") \
            .withColumnRenamed("_12","person") \
            .withColumnRenamed("_16","tone") \
            .select('rec_id','file_date','theme','person','tone')

        split_col = F.split(gkgDF['tone'], ',')
        gkgDF = gkgDF.withColumn('ave_tone', split_col.getItem(0)) \
            .withColumn('postivie_score', split_col.getItem(1)) \
            .withColumn('negative_score', split_col.getItem(2)) \
            .withColumn('polarity', split_col.getItem(3)) \
            .select('rec_id','file_date','person','theme','ave_tone','postivie_score','negative_score','polarity') 

        logging.info('after spliting tone')
        logging.info(gkgDF.head(1))
        
        #Store in parquet format on S3
        gkgDF.write.mode('append').parquet('s3://gdeltstorage/gdelt_parquet') #'s3://gdeltstorage/gdelt_parquet'
            
        gkgDF = gkgDF \
            .select('rec_id',explode(split(col('person'), ';')).alias('person'),
                    'file_date','theme','ave_tone','postivie_score',
                    'negative_score','polarity') \
            .select('rec_id','person', 
                    explode(split(col('theme'), ';')).alias('theme'),
                    'file_date','ave_tone','postivie_score',
                    'negative_score','polarity') \

        logging.info('after exploding')
        logging.info(gkgDF.head(3))
        gkgDF = gkgDF.filter(("person like '%bernie sanders%' or \
                              person like '%joe biden%' or \
                              person like '%amy klobuchar%' or \
                              person like '%michael bloomberg%' or \
                              person like '%pete buttigieg%' or \
                              person like '%tom steyer%' or \
                              person like '%donald trump%' ")) \
            .select('rec_id','file_date','theme','person','ave_tone','postivie_score','negative_score','polarity')
            
        logging.info('after filtering presidential candidates')
        logging.info(gkgDF.head(3))
                
        Utils.save_to_db(gkgDF, 'dbtable_elections') 

    def main(self):
        """
        Extracts extracts candidate related publications from csv file to stores in elections table
        """
        
        logging.info('Started main()')
        sqlContext = SQLContext(self.sc)
        inputFile = 's3a://gdeltstorage/currentGdeltFile.csv'
        gkgRDD = self.sc.textFile(inputFile) 
        gkgRDD = gkgRDD.map(lambda x: x.encode('utf', 'ignore'))
        gkgRDD.cache()
        gkgRDD = gkgRDD.map(lambda x: x.split('\t'))

        gkgDF = sqlContext.createDataFrame(gkgRDD)
        self.build_elections_sentiment_explorer(gkgDF)        
        
        logging.info('Finished main()')

#######################################################
if __name__ == '__main__':
   ggfi = Gdelt_gkg_file_ingester_elections()
   ggfi.main()