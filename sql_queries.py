import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get('S3','LOG_DATA')
SONG_DATA=config.get('S3','SONG_DATA')
ARN=config.get('IAM_ROLE','ARN')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""create table if not exists staging_events  (artist varchar,auth varchar, firstName varchar,gender varchar ,itemlnSession int ,lastName varchar,length numeric,level varchar,location varchar,method varchar,page varchar,registration numeric,sessionId int ,song varchar,status int,ts timestamp,userAgent varchar,userId int);""")

staging_songs_table_create = ("""create table if not exists staging_songs (num_songs int,artist_id varchar not null,artist_latitude numeric,artist_longitude numeric,artist_location varchar,artist_name varchar,song_id varchar,title varchar,duration numeric,year int);""")

songplay_table_create ="create table if not exists songplays (songplay_id bigint IDENTITY(0,1) primary key not null ,start_time timestamp not null,user_id int4 not null references users(user_id), level varchar,song_id varchar not null references songs(song_id),artist_id varchar not null references artists(artist_id)  ,session_id int4,location varchar,user_agent varchar)"

user_table_create = "create table if not exists users (user_id int4 primary key,first_name varchar,last_name varchar,gender varchar,level varchar);"

song_table_create = "create table if not exists songs (song_id varchar primary key,title varchar,artist_id varchar not null,year int4,duration numeric);"

artist_table_create = "create table if not exists artists (artist_id varchar primary key,name varchar,location varchar,lattitude numeric,longitude numeric);"

time_table_create ="create table if not exists time  (start_time timestamp primary key,hour int4,day int4,week int4,month int4,year int4,weekday int4);"


# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} \n
credentials'aws_iam_role={}' region 'us-west-2' format as JSON{} TIMEFORMAT as 'epochmillisecs' ;""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {} \n
credentials'aws_iam_role={}' region 'us-west-2' format as JSON 'auto';""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select 
public.staging_events.ts as start_time,
public.staging_events.userid as user_id,
public.staging_events.level as level,
public.staging_songs.song_id as song_id,
public.staging_songs.artist_id as artist_id,
public.staging_events.sessionid as session_id,
public.staging_events.location as location,
public.staging_events.useragent as user_agent
from public.staging_events
join public.staging_songs on (public.staging_events.artist=public.staging_songs.artist_name and
public.staging_events.song=public.staging_songs.title)
where public.staging_events.page='NextSong';""")

# INSERT TABLE 
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
select DISTINCT userid as user_id,
firstname as first_name,
lastname as last_name,
gender,
level
from staging_events 
where userid notnull;""")

# INSERT TABLE 
song_table_insert = ("""INSERT INTO songs  (song_id, title, artist_id, year, duration)
select DISTINCT song_id as song_id,
 title,
 artist_id,
 year,
 duration
 from staging_songs;""")

# INSERT TABLE 
artist_table_insert = ("""INSERT INTO artists  (artist_id, name, location, lattitude, longitude)
select DISTINCT artist_id as artist_id,
artist_name as name,
artist_location as location,
artist_latitude as lattitude,
artist_longitude as longitude
 from staging_songs;""")

# INSERT TABLE 
time_table_insert = ("""insert into time (start_time ,hour ,day ,week ,month ,year ,weekday )
select start_time,
EXTRACT(hour FROM start_time) as hour,
EXTRACT(day FROM start_time) as day,
EXTRACT(week FROM start_time) as week,
EXTRACT(month FROM start_time) as month,
EXTRACT(year FROM start_time) as year,
EXTRACT(weekday FROM start_time) as weekday
from public."songplays";""")

# QUERY LISTS
create_table_queries = [ user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create ,staging_events_table_create,staging_songs_table_create]
drop_table_queries = [songplay_table_drop,user_table_drop, song_table_drop, artist_table_drop, time_table_drop ,staging_events_table_drop,staging_songs_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert, time_table_insert]


