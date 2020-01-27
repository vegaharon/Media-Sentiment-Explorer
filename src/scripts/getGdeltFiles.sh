#!/bin/bash
#This script downloads a gdelt file from the source, unzips it and saves in the S3 storage
#Files are processed in a loop one at a time from the given start date to the end date.

startDate="2019-12-30"
endDate="2020-01-02"

path="http://data.gdeltproject.org/gdeltv2/"
fileExt=".gkg.csv.zip"
fileExtUnziped=".gkg.csv"

((maxminutes=24*60))
while [ "$startDate" != "$endDate" ]; do
  startDate=$(date -I -d "$startDate + 1 day")
  for ((m=0; m<${maxminutes};m+=240)); do
     fileName=$(date -d "${startDate} + ${m} minutes" +"%Y%m%d%H%M%S")
         file=$path$fileName$fileExt
         wget $file
         unzip $fileName$fileExt
         rm $fileName$fileExt
         aws s3 sync . s3://gdeltstorage --exclude "*.sh"
         rm $fileName$fileExtUnziped
  done
done
