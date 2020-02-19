# -*- coding: utf-8 -*-
"""
Created on Sun Feb  2 00:33:02 2020

@author: Vegan Aharonian
"""

import configparser
import logging

def parse_input_file_into_rdd(sc): 
    inputFile = 's3a://gdeltstorage/currentGdeltFile.csv'
    gkgRDD = sc.textFile(inputFile) 
    gkgRDD = gkgRDD.map(lambda x: x.encode('utf', 'ignore'))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))

    return gkgRDD

def save_to_s3_parquet(gkgDF, destination):
    gkgDF.write.mode('append').parquet(destination)

def save_to_db(df, dbtable):
    
    logging.info('Starting to insert into table: ' + dbtable)
    
    config = configparser.ConfigParser()
    config.read('../../config/gdelt.ini')
                
    dbParams =  {'url' : config['PostgreSQL']['url']
                 ,'dbtable_staging' : config['PostgreSQL']['dbtable_staging']
                 ,'dbtable_elections' : config['PostgreSQL']['dbtable_elections']
                 ,'dbtable_persons' : config['PostgreSQL']['dbtable_persons']
                 ,'dbtable_organizations' : config['PostgreSQL']['dbtable_organizations']
                 ,'dbtable_themes' : config['PostgreSQL']['dbtable_themes']
                 ,'dbtable_tones' : config['PostgreSQL']['dbtable_tones']
                 ,'user' : config['PostgreSQL']['user']
                 ,'password' : config['PostgreSQL']['password']
                 ,'stringtype' : config['PostgreSQL']['stringtype']
                 ,'driver' : config['PostgreSQL']['driver']
                 }

    try:
        df.write.format('jdbc') \
        .option('url', dbParams['url']) \
        .option('dbtable', dbParams[dbtable]) \
        .option('user', dbParams['user']) \
        .option('password', dbParams['password']) \
        .option('stringtype', dbParams['stringtype']) \
        .option('driver', dbParams['driver']) \
        .option('batchsize', 10000) \
        .mode('append') \
        .save()
    except:
        logging.error('An error occured while inserting into table: ' + dbtable)

    logging.info('Finished inserting into table: ' + dbtable)
