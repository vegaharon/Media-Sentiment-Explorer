#!/bin/bash
#This script downloads a gdelt file from the source, unzips it and saves in the S3 storage
#Files are processed in a loop one at a time from the given start date to the end date.
#Then it executes python spark job that ingests that file from S3 converts it to dataframe
#selects some of the columns, renames them and archives them in S3 in parquet format
#It also transforms it and saves in postgres DB
#Then an SQL function is executed in postgres that does to further ths data processing to prepare it for 
#further usage by a DS model or a front end dashboard. 
#intermediae files get delited as they become unnesessary

startDate="2020-01-31"
endDate="2020-02-02"

gdeltPath="http://data.gdeltproject.org/gdeltv2/"
fileExt=".gkg.csv.zip"
fileExtUnziped=".gkg.csv"
localPath="~/gitRoot/data/gdelt/"
defaultFileName="currentGdeltFile.csv"

start_time=`date +%s`
echo $start_time'  starting the shell'  >> timing.log
current_time=`date +%s`
((maxminutes=24*60))
while [ "$startDate" != "$endDate" ]; do
  startDate=$(date -I -d "$startDate + 1 day")
  for ((m=0; m<${maxminutes};m+=15)); do
     start_time_loop=`date +%s`
     fileName=$(date -d "${startDate} + ${m} minutes" +"%Y%m%d%H%M%S")
         file=$gdeltPath$fileName$fileExt
		 #download, unzip and store the input file in S3
         wget $file
         unzip $fileName$fileExt
         mv $fileName$fileExtUnziped /home/ubuntu/gitRoot/data/gdelt/$defaultFileName
         aws s3 mv /home/ubuntu/gitRoot/data/gdelt/currentGdeltFile.csv s3://gdeltstorage --exclude "*.sh"
         rm $fileName$fileExt
         #execute spark code to transform the data 
         #spark-submit --master spark://ip-10-0-0-13:7077 --packages org.postgresql:postgresql:42.2.9 --jars /home/ubuntu/lib/postgresql-42.2.9.jar ~/gitRoot/Media-Sentiment-Explorer/src/python/Gdelt_gkg_file_ingester_elections.py
         spark-submit --packages org.postgresql:postgresql:42.2.9 --jars /home/ubuntu/lib/postgresql-42.2.9.jar Gdelt_gkg_file_ingester_elections.py
         current_time=`date +%s`
         runtime=$((current_time-start_time_loop))
         echo $runtime'  after spark submit' >> timing.log
		 #exeucute an SQL function to further process the data
         ssh -i ~/.ssh/id_rsa ip-10-0-0-4.ec2.internal 'sudo -u postgres -i -H -- psql -c "SELECT populate_gdelt()"'
         current_time1=`date +%s`
         runtime=$((current_time1-current_time))
         echo $runtime'  after populate_gdelt()'  >> timing.log
  done
done
runtime=$((current_time1-start_time))
echo $runtime'  total time'  >> timing.log
