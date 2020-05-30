# Overview:

Sparkify is a music streaming app used by users to listen songs. Sparkify is collecting data about user activity and songs through their app on AWS S3. Now they want to use this collected data to create a data warehouse on Redshift which will help them analyse songs and user activity and in turn provide better user experience.

# Source Files:

1) Song Files - which holds information about song metadata along with artist details
2) Log Files - which holds details about user activity 

# Requirements:

Use source files to create a star schema datawarehouse for Sparkify in AWS Redshift.
Details about source files and final star schema data model is shown in separate tab in AWS Redshift Data Model.xlsx

# Specifications:

- Create Redshift Cluster and EC2 Instance
- Create Staging, Dimension and Fact tables
- Copy song and log files into staging tables i.e. staging_events and staging_songs
- Insert records from staging tables into Dimensions and Fact

# Project Details:
Following is the script order to run this project:

1) sql_queries.py - This script includes queries as variables for drop,create and insert tables. No need to run this.
2) create_tables.py - This script creates and calls functions for creating db connections, droping tables and creating tables. This is the first script to be run.
3) etl.py - This is the script which will process AWS files from S3 and inserts records into staging tables and then into dimensions and fact


# Following Analysis queries can be run with designed datawarehouse to answer key questions about songs and artists

## Most Listened Songs

- select sp.song_id,s.title,count(*) as most_listened_songs from songplays as sp left join songs as s on sp.song_id=s.song_id group by 1,2 order by 3 desc

## Most Listened Artists

- select sp.artist_id,a.artist_name,count(*) as most_famous_artist from songplays as sp left join artists as a on sp.artist_id=a.artist_id group by 1,2 order by 3 desc

## Most famous songs by year (this queries will need row_number function to get max value to filter)

- select t.year,s.title,count(*) as most_famour_song_by_year from songplays as sp left join songs as s on sp.song_id=s.song_id left join time as t on sp.start_time=t.start_time group by 1,2 order by 3 desc


Similar queries can be written to get:

1) Most famous artist by year/month/week etc.

2) Most popular song by users

3) Most popular song by artist

