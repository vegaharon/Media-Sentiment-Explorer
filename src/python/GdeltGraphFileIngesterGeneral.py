# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 17:09:27 2020

@author: Vegan Aharonian
"""

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, explode, split
import logging
import Utils

class Gdelt_gkg_file_ingester:
    """
    Handles ingests GDELT graph data file into staging table
    """

    def __init__(self):
        """
        Setting up Spark session and Spark context, AWS access key
        """
        logging.basicConfig(filename='gdelt.log', 
                            format='%(asctime)s %(levelname)s: %(message)s ', 
                            level=logging.INFO)
        logging.info('STARTING Gdelt_gkg_file_ingester')
        
        spark = SparkSession.builder \
            .appName('bubble-breaker') \
            .getOrCreate()
    
        self.sc=spark.sparkContext
        self.sc.setLogLevel("WARN")
        
    def _ingest_tones(self):
        """
        Extracts tones from csv file to store in tones table
        """
        logging.info('Starting tones ingester.')

        split_col = F.split(self.gkgDF['tone'], ',')
        tones_df = self.gkgDF.withColumn('ave_tone', split_col.getItem(0)) \
            .withColumn('postivie_score', split_col.getItem(1)) \
            .withColumn('negative_score', split_col.getItem(2)) \
            .withColumn('polarity', split_col.getItem(3))
        
        tones_df = tones_df.select('file_date','publication_id','ave_tone',
                             'postivie_score','negative_score','polarity') 

        logging.info(tones_df.take(1))

        Utils.save_to_db(tones_df, 'dbtable_tones')
        
    def _ingest_staging(self):
        """
        Extracts staging from csv file to store in staging table
        """

        self.gkgDF = self.gkgDF \
            .select('publication_id',explode(split(col('person'), ';'))
                    .alias('person'),'file_date','source_collection_id',
                    'theme','organization') \
            .select('publication_id','person', explode(split(col('theme'), ';'))
                    .alias('theme'),'file_date',
                    'source_collection_id','organization') \
            .select('publication_id','person','theme',
                    explode(split(col('organization'), ';'))
                    .alias('organization'),
                    'file_date','source_collection_id') 
        
        logging.info('print(staging: expl_gdelt_df.take(1))')
        logging.info(self.gkgDF.take(1))

        #Insert into table
        Utils.save_to_db(self.gkgDF, 'dbtable_staging')   

    def _rename_select_df_columns(self):        
        self.gkgDF = self.gkgDF \
            .withColumnRenamed("_1","rec_id") \
            .withColumnRenamed("_2","file_date") \
            .withColumnRenamed("_3","source_collection_id") \
            .withColumnRenamed("_8","theme") \
            .withColumnRenamed("_12","person") \
            .withColumnRenamed("_14","organization") \
            .withColumnRenamed("_16","tone") \
            .select('rec_id','file_date','source_collection_id','theme','person','organization','tone')
    
    def _append_an_id_column(self):  
        self.gkgDF = self.gkgDF.select('rec_id','file_date',
            'source_collection_id','theme','person','organization','tone', 
            F.row_number()
            .over(Window.partitionBy('file_date')
            .orderBy("rec_id"))
            .alias("publication_id"))
            
    def main(self):
        """
        Extracts persons from csv file to store in persons table
        """
        
        logging.info('Started main()')
        sqlContext = SQLContext(self.sc)
        gkgRDD = Utils.parse_input_file_into_rdd(self.sc)

        self.gkgDF = sqlContext.createDataFrame(gkgRDD)
        self._rename_select_df_columns()
        self._append_an_id_column()        
        
        logging.info(self.gkgDF.head(3))
        #save raw complete data to parquet file for future use
        self.gkgDF.write.mode('append').parquet('gdelt_parquet')
        Utils.save_to_s3_parquet(self.gkgDF, 
                                 's3://gdelts_general_storage/gdelt_parquet')

        self.ingest_tones()
        self.ingest_staging()
        logging.info('Finished main()')

#######################################################
if __name__ == '__main__':
    ggfi = Gdelt_gkg_file_ingester()
    ggfi.main()