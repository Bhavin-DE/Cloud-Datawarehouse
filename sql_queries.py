import configparser
 
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
 
# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"
 
# Staging tables - Create table queries
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_EVENTS (artist varchar(300), \
                                                                            auth varchar(25), \
                                                                            firstName varchar(50), \
                                                                            gender varchar(1), \
                                                                            iteminSession int, \
                                                                            lastName varchar(50), \
                                                                            length DECIMAL(8,4), \
                                                                            level varchar(25), \
                                                                            location varchar(250), \
                                                                            method varchar(20), \
                                                                            page varchar(20), \
                                                                            registration BIGINT, \
                                                                            sessionid int, \
                                                                            song varchar(500), \
                                                                            status int, \
                                                                            ts BIGINT, \
                                                                            userAgent varchar(250), \
                                                                            userid varchar(10));""")
 
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_SONGS (num_songs int, \
                                                                           artist_id varchar(100), \
                                                                           artist_latitude DECIMAL(8,4), \
                                                                           artist_longitude DECIMAL(8,4),\
                                                                           artist_location varchar(500), \
                                                                           artist_name varchar(500), \
                                                                           song_id varchar(25), \
                                                                           title varchar(500), \
                                                                           duration VARCHAR(25), \
                                                                           year int);""")
 
# Dimensions and Facts - Create table queries
user_table_create = ("""CREATE TABLE IF NOT EXISTS USERS(userid varchar(10) not null, \
                                                         firstName varchar(50), \
                                                         lastName varchar(50), \
                                                         gender varchar(1), \
                                                         level varchar(25)) \
                                                         sortkey(userid);""")
 
song_table_create = ("""CREATE TABLE IF NOT EXISTS SONGS(song_id varchar(25), \
                                                         title varchar(500), \
                                                         artist_id varchar(100), \
                                                         year int, \
                                                         duration varchar(25)) \
                                                         sortkey(song_id);""")
 
artist_table_create = ("""CREATE TABLE IF NOT EXISTS ARTISTS(artist_id varchar(100), \
                                                             artist_name varchar(500), \
                                                             location varchar(500), \
                                                             latitude DECIMAL(8,4), \
                                                             longitude DECIMAL(8,4)) \
                                                             sortkey(artist_id);""")
 
time_table_create = ("""CREATE TABLE IF NOT EXISTS TIME(start_time timestamp, \
                                                          hour int, \
                                                          day int, \
                                                          week int, \
                                                          month int, \
                                                          year int, \
                                                          weekday int) \
                                                          sortkey(start_time);""")
 
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS SONGPLAYS(songplay_id INT IDENTITY(0,1), \
                                                          start_time timestamp, \
                                                          userid varchar(10), \
                                                          level varchar(25), \
                                                          song_id varchar(25), \
                                                          artist_id varchar(100), \
                                                          sessionid int, \
                                                          location varchar(250), \
                                                          useragent varchar(250)) \
                                                          distkey(song_id) sortkey(start_time);""")
 
# STAGING TABLES - Copy Queries
staging_events_copy = ("""COPY STAGING_EVENTS FROM 's3://udacity-dend/log_data/2018/11' \
CREDENTIALS 'aws_iam_role={}' json  {} """).format(config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))
                   
staging_songs_copy = ("""COPY STAGING_SONGS FROM 's3://udacity-dend/song-data' \
CREDENTIALS 'aws_iam_role={}' json 'auto' """).format(config.get("IAM_ROLE","ARN"))
 
# Dimensions and Facts - Insert Queries
user_table_insert = ("INSERT INTO USERS (userid, \
                                         firstName, \
                                         lastName, \
                                         gender, \
                                         level) \
                                         (select distinct userid, \
                                                          firstName, \
                                                          lastName, \
                                                          gender, \
                                                          level \
                                          from staging_events)")
 
song_table_insert = ("INSERT INTO SONGS (song_id, \
                                         title, \
                                         artist_id, \
                                         year, \
                                         duration) \
                                         (select distinct song_id, \
                                                          title, \
                                                          artist_id, \
                                                          year, \
                                                          duration \
                                          from staging_songs)")
  
artist_table_insert = ("INSERT INTO ARTISTS (artist_id, \
                                             artist_name, \
                                             location, \
                                             latitude, \
                                             longitude) \
                                             (select distinct artist_id, \
                                                              artist_name, \
                                                              artist_location, \
                                                              artist_latitude, \
                                                              artist_longitude \
                                              from staging_songs)")
 
time_table_insert = ("INSERT INTO TIME (start_time, \
                                        hour, \
                                        day, \
                                        week, \
                                        month, \
                                        year, \
                                        weekday) \
                                        (select distinct a.start_time, \
                                                         extract(hour from a.start_time) as hour, \
                                                         extract(day from a.start_time) as day, \
                                                         extract(week from a.start_time) as week, \
                                                         extract(month from a.start_time) as month, \
                                                         extract(year from a.start_time) as year, \
                                                         extract(weekday from a.start_time) as weekday \
                                         from  (select timestamp 'epoch' + (ts/1000) * interval '1 second' AS start_time \
                                                from staging_events) as a)")
                                   
songplay_table_insert = ("INSERT INTO SONGPLAYS(start_time, \
                                                userid, \
                                                level, \
                                                song_id, \
                                                artist_id, \
                                                sessionid, \
                                                location, \
                                                useragent) \
                                                (select distinct timestamp 'epoch' + (e.ts/1000) * interval '1 second' AS start_time, \
                                                        e.userid, \
                                                        e.level, \
                                                        s.song_id, \
                                                        s.artist_id, \
                                                        e.sessionid, \
                                                        e.location, \
                                                        e.useragent \
                                                 from staging_events as e \
                                                      left join \
                                                      staging_songs  as s \
                                                      on e.song=s.title and e.artist=s.artist_name \
                                                      and e.page='NextSong')")
 
# Put queries into List variables
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
 
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
 
copy_table_queries = [staging_events_copy, staging_songs_copy]
 
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]