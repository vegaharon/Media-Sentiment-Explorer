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

class GdeltGraphFileIngesterElections:
    """
    Ingests GDELT graph data file. Takes a slice related to presidential elections. 
    """

    def __init__(self):
        """
        Setting up Spark session and Spark context, AWS access key
        """
        logging.basicConfig(filename='gdelt.log', 
                            format='%(asctime)s %(levelname)s: %(message)s ', 
                            level=logging.INFO)
        logging.info('STARTING GdeltGraphFileIngesterElections')
        
        spark = SparkSession.builder \
            .appName('bubble-breaker') \
            .getOrCreate()
    
        self.sc=spark.sparkContext
        self.sc.setLogLevel("WARN")
        
    def _rename_select_df_columns(self):        
        self.gkgDF = self.gkgDF \
            .withColumnRenamed("_1","rec_id") \
            .withColumnRenamed("_2","file_date") \
            .withColumnRenamed("_8","theme") \
            .withColumnRenamed("_12","person") \
            .withColumnRenamed("_16","tone") \
            .select('rec_id','file_date','theme','person','tone')

    def _split_tone_column(self):        
        split_col = F.split(self.gkgDF['tone'], ',')
        self.gkgDF = self.gkgDF.withColumn('ave_tone', split_col.getItem(0)) \
            .withColumn('postivie_score', split_col.getItem(1)) \
            .withColumn('negative_score', split_col.getItem(2)) \
            .withColumn('polarity', split_col.getItem(3)) \
            .select('rec_id','file_date','person','theme','ave_tone',
                    'postivie_score','negative_score','polarity') 
    
    def _explode_person_and_theme_columns(self):        
        self.gkgDF = self.gkgDF \
            .select('rec_id',explode(split(col('person'), ';')).alias('person'),
                    'file_date','theme','ave_tone','postivie_score',
                    'negative_score','polarity') \
            .select('rec_id','person', 
                    explode(split(col('theme'), ';')).alias('theme'),
                    'file_date','ave_tone','postivie_score',
                    'negative_score','polarity') 

    def _filter_current_election_candidates(self):        
        self.gkgDF = self.gkgDF.filter(("person like '%bernie sanders%' or \
                              person like '%joe biden%' or \
                              person like '%amy klobuchar%' or \
                              person like '%michael bloomberg%' or \
                              person like '%pete buttigieg%' or \
                              person like '%tom steyer%' or \
                              person like '%donald trump%' ")) \
            .select('rec_id','file_date','theme','person','ave_tone',
                    'postivie_score','negative_score','polarity')

    def _build_elections_sentiment_explorer(self):   
        
        self._rename_select_df_columns()
        self._split_tone_column()

        logging.info('after spliting tone')
        logging.debug(self.gkgDF.head(1))
        
        #Store in parquet format on S3
        Utils.save_to_s3_parquet(self.gkgDF, 
                                 's3://gdelt_elections_storage/gdelt_parquet')
            
        self._explode_person_and_theme_columns() 

        logging.info('after exploding')
        logging.debug(self.gkgDF.head(3))
        self._filter_current_election_candidates()
        
        logging.info('after filtering presidential candidates')
        logging.debug(self.gkgDF.head(3))
                
        Utils.save_to_db(self.gkgDF, 'dbtable_elections')
        
    def main(self):
        """
        Extracts extracts candidate related publications from csv file to 
        stores in elections table
        """
        
        sqlContext = SQLContext(self.sc)

        gkgRDD = Utils.parse_input_file_into_rdd(self.sc)
        self.gkgDF = sqlContext.createDataFrame(gkgRDD)
        self._build_elections_sentiment_explorer()        
        
        logging.info('Finished main()')

#######################################################
if __name__ == '__main__':
   ggfi = GdeltGraphFileIngesterElections()
   ggfi.main()