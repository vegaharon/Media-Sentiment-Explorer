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
    Handles GDELT graph data file
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
        
    def ingest_persons(self):
        """
        Extracts persons from csv file to store in persons table
        Returns
        -------
        None.
    
        """
        logging.info('Starting persons ingester.')

        expl_gdelt_df = self.gdelt_df.select('person') \
                        .select(explode(split(col('person'), ';'))
                        .alias('person')) \
                        .filter("person != ''") \
                        .distinct()

        Utils.save_to_db(expl_gdelt_df, 'dbtable_persons')

    def ingest_organizations(self):
        """
        Extracts organizations from csv file to store in organizations table
    
        Returns
        -------
        None.
    
        """
        logging.info('Starting organizations ingester.')
        dimention = 'organization'

        expl_gdelt_df = self.gdelt_df.select(dimention) \
                        .select(explode(split(col(dimention), ';'))
                        .alias(dimention)) \
                        .filter("organization != ''") \
                        .distinct()

        Utils.save_to_db(expl_gdelt_df, 'dbtable_organizations')

    def ingest_tones(self):
        """
        Extracts tones from csv file to store in tones table

        Returns
        -------
        None.

        """
        logging.info('Starting tones ingester.')

        df = self.gdelt_df
        split_col = F.split(df['tone'], ',')
        df = df.withColumn('ave_tone', split_col.getItem(0)) \
            .withColumn('postivie_score', split_col.getItem(1)) \
            .withColumn('negative_score', split_col.getItem(2)) \
            .withColumn('polarity', split_col.getItem(3))
        
        tones_df = df.select('file_date','publication_id','ave_tone','postivie_score','negative_score','polarity') 

        logging.info(tones_df.take(1))

        Utils.save_to_db(tones_df, 'dbtable_tones')
        
    def ingest_staging(self):
        """
        Extracts staging from csv file to store in staging table

        Returns
        -------
        None.
    
        """

        expl_gdelt_df = self.gdelt_df \
            .select('publication_id',explode(split(col('person'), ';')).alias('person'),'file_date','source_collection_id','theme','organization') \
            .select('publication_id','person', explode(split(col('theme'), ';')).alias('theme'),'file_date','source_collection_id','organization') \
            .select('publication_id','person','theme',explode(split(col('organization'), ';')).alias('organization'),'file_date','source_collection_id') 
        
        logging.info('print(staging: expl_gdelt_df.take(1))')
        logging.info(expl_gdelt_df.take(1))

        #Insert into table
        Utils.save_to_db(expl_gdelt_df, 'dbtable_staging')   
        
    def main(self):
        """
        Extracts persons from csv file to store in persons table
        Returns
        -------
        None.
    
        """
        
        logging.info('Started main()')
        sqlContext = SQLContext(self.sc)
        inputFile = 's3a://gdeltstorage/currentGdeltFile.csv'

        gkgRDD = self.sc.textFile(inputFile) 
        gkgRDD = gkgRDD.map(lambda x: x.encode('utf', 'ignore'))
        gkgRDD.cache()
        gkgRDD = gkgRDD.map(lambda x: x.split('\t'))

        gkgDF     = sqlContext.createDataFrame(gkgRDD)

        gdelt_df_renamed = gkgDF \
            .withColumnRenamed("_1","rec_id") \
            .withColumnRenamed("_2","file_date") \
            .withColumnRenamed("_3","source_collection_id") \
            .withColumnRenamed("_8","theme") \
            .withColumnRenamed("_12","person") \
            .withColumnRenamed("_14","organization") \
            .withColumnRenamed("_16","tone") \
            .select('rec_id','file_date','source_collection_id','theme','person','organization','tone')

        gdelt_df_renamed = gdelt_df_renamed.select('rec_id','file_date',
            'source_collection_id','theme','person','organization','tone', 
            F.row_number()
            .over(Window.partitionBy('file_date')
            .orderBy("rec_id"))
            .alias("publication_id"))
        
        
        logging.info(gdelt_df_renamed.head(3))
        #save raw complete data to parquet file for future use
        gdelt_df_renamed.write.mode('append').parquet('gdelt_parquet')
        
        self.gdelt_df = gdelt_df_renamed
        #self.ingest_persons()
        #self.ingest_organizations()
        self.ingest_tones()
        self.ingest_staging()
        logging.info('Finished main()')

#######################################################
if __name__ == '__main__':
    ggfi = Gdelt_gkg_file_ingester()
    ggfi.main()