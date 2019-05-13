Purpose:

Build a data warehouse for analytical operations. to allow Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app
Benefits


Design schema
there is two table stage, this table will copy data from S3 to the table stage.
Star schema because it's more effective for handling queries

Fact Table

songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

Etl

the ETL file is tor drop tables if exist and create tables, after that, the script will copy and insert data in tables

load_staging_tables function:
this function to load two-stage table from S3 to redshift.

insert_tables
this function inserts data from stage tables to star schema tables fact and daimantion.


to start this project follow the below step:

1-create cluster redshift in AWS and add value in dwh.cfg.
2-run create_tables to create tables.
2-run est file to load data to tables 


SQL example from songplays

select * from public."songplays"
limit 10;




