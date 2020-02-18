# Media Sentiment Explorer
 > ***Fore-informed  forearmed***
 
 My Insight Data Engineering project (New York, winter 2020). 
 The elections part of the project can be watched [here](https://public.tableau.com/profile/aharonian#!/vizhome/MediaSentimentsFor2020electioncandidates/Elections2020?publish=yes) or when my AWS server is running [datamindful.us](http://www.datamindful.us/).
 
 ***
 The goal of this project is to build a pipeline that would help to correlate media sentiments 
 with other measurable fenomena of interest for DS or AI projects.
 To demonstarate the potential of the pipelene we took a slice of data related to 2020 presidentail elections and 
 built a dashboard that demontrates media coverage of current presidential candidates. 
 This will allow to strategise the presidential campaign, which is expected to reach 6 billion in 2020, based on sound DS research. 
 
 Another potential use of the pipeline is to study the effects of marketing campaigns. In 2020 the spending on adds on the major platforms is expected to reach 33 billion. 
 
 There are many more areas of analisis that can benefit from using this pipline. 
 
 ***
 # Data Source
Global Database of Events, Language, and Tone (GDELT)

GDELT is a very rich and informtive dataset with a huge potential that is described as: 
> "an initiative to construct a catalog of human societal-scale behavior and beliefs across all countries of the world, connecting every person, organization, location, count, theme, news source, and event across the planet into a single massive network that captures what's happening around the world, what its context is and who's involved, and how the world is feeling about it, every single day."

# GDELT:
  - Monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages
  - Identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day
  - Provides four sentiment attributes for each publication: Average tone, Number of positive words,  Number of negative words, and Polarity.  
 ***
 # A snapshot of 2020 presidential campaign media sentiments
 

> * The dashboard shows the comparison betweeen candidates for all the availble sentiment parameters. *

 ![elections](https://github.com/vegaharon/Media-Sentiment-Explorer/blob/dev/docs/EelctionsSnapshot.JPG)
 
 ***
 
# Pipeline

- The bash script downloads a gdelt gkg file from the source, unzips it and saves in the AWS S3 storage. 
- Files are processed in a loop one at a time every 15 min as they become available or for archoved data 
from the given start date to the end date.
- Then python spark job is executed to ingest that file from S3 convert it to dataframe
select some of the columns, rename them and archive in S3 in parquet format for potential future use. 
- Spark code also transforms it by exploding to have an atomic data point per row and saves in postgres DB. 
- After that an SQL function is executed in postgres. It performs further data normalization processing to prepare it for 
usage by a DS model or a visualization dashboard. 
- Intermediate files get delited as they become unnesessary. 

This script downloads 
 ![pipiline](https://github.com/vegaharon/Media-Sentiment-Explorer/blob/dev/docs/Pipeline.JPG)

***
# Setup

Clone this repository using
'git clone https://github.com/vegaharon/Media-Sentiment-Explorer'.

#### CLUSTER STRUCTURE:

To reproduce my environment, 11 m4.large AWS EC2 instances are needed:

- (4 nodes) Spark Cluster (1 master and 3 workers)
- 1 Postgres Node
- Web server node

##### Airflow setup

The Apache Airflow can be installed on the master node of *spark-batch-cluster*. Follow the instructions in `docs/Airflow_instructions.txt` to launch the Airflow server.

##### PostgreSQL setup
The PostgreSQL database sits on the db node.
Follow the instructions in `docs/postgres_instructions.txt` to download and setup access to it.

##### Configurations
Configuration settings are stored in `config/` folder and not uploaded to git for sequrity reasons.
>  The parmeters are expected in `config/gdelt.ini` with table names and password to the postgres DB.

##### Sample input file
 ![a gkg zip file](https://github.com/vegaharon/Media-Sentiment-Explorer/blob/dev/docs/20200201174500.gkg.csv.zip)

