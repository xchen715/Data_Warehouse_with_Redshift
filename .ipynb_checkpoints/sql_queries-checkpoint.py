import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('/home/wilson/Data_Engineering_NanoDegree/AWS_Keys/dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
LOG_DATA=config.get('S3','LOG_DATA')
SONG_DATA=config.get('S3','SONG_DATA')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS dimuser"
song_table_drop = "DROP TABLE IF EXISTS dimsong"
artist_table_drop = "DROP TABLE IF EXISTS dimartist"
time_table_drop = "DROP TABLE IF EXISTS dimtime"

# CREATE TABLES

staging_events_table_create= (""" 
CREATE TABLE IF NOT EXISTS staging_events 
(
eventId            INT IDENTITY(0,1),
artist             VARCHAR(2000),
auth               TEXT,
firstName          TEXT,
gender             TEXT,
itemInSession      INT,
lastName           TEXT,
length             FLOAT,
level              TEXT,
location           VARCHAR(2000),
method             TEXT,
page               TEXT,
registration       FLOAT,
sessionId          INT,
song               VARCHAR(2000),
status             INT,
ts                 TEXT,
userAgent          VARCHAR(2000),
userId             INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            TEXT,
num_songs          INT,
artist_id          TEXT,
artist_latitude    FLOAT,
artist_longitude   FLOAT,
artist_location    VARCHAR(65535),
artist_name        VARCHAR(65535),
title              VARCHAR(2000),
duration           FLOAT,
year               INT
)
""")

## Fact Table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
songplay_id        INT IDENTITY(0,1),
start_time         TIMESTAMP,
user_id            INT,
level              TEXT,
song_id            TEXT,
artist_id          TEXT,
session_id         INT,
location           VARCHAR(2000),
user_agent         VARCHAR(2000),
PRIMARY KEY (songplay_id),
FOREIGN KEY (start_time) REFERENCES dimTime(start_time),
FOREIGN KEY (user_id) REFERENCES dimUser(user_id),
FOREIGN KEY (song_id) REFERENCES dimSong(song_id),
FOREIGN KEY (artist_id) REFERENCES dimArtist(artist_id)
DISTKEY (start_time)
SORTKEY (songplay_id)
)
""")

## Dimension Tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimUser
(
user_id            INT NOT NULL,
first_name         TEXT,
last_name          TEXT,
gender             TEXT,
level              TEXT,
PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dimSong
(
song_id           TEXT NOT NULL,
title             VARCHAR(2000),
artist_id         TEXT NOT NULL,
year              INT,
duration          FLOAT,
PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dimArtist
(
artist_id         TEXT NOT NULL,
name              VARCHAR(65535),
location          VARCHAR(65535),
lattitude         FLOAT,
longitude         FLOAT,
PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimTime
(
start_time        TIMESTAMP NOT NULL,
hour              INT,
day               INT,
week              INT,
month             INT,
year              INT,
weekday           INT
PRIMARY KEY (start_time)
DISTKEY (start_time)
)
""")

# LOAD DATA INTO CLUSTER
# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
json {} region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
json 'auto' region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT 
TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
e.userId,
e.level,
s.song_Id,
s.artist_Id,
e.sessionId,
e.location,
e.userAgent
FROM staging_events e, staging_songs s
WHERE e.song = s.title
AND e.artist = s.artist_name
AND e.length = s.duration
AND e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dimUser (user_id,first_name,last_name,gender,level)
SELECT
userId,
firstName,
lastName,
gender,
level
FROM staging_events;
""")

song_table_insert = ("""
INSERT INTO dimSong (song_id,title,artist_id,year,duration)
SELECT
song_id,
title,
artist_id,
year,
duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO dimArtist (artist_id,name,location,lattitude,longitude)
SELECT
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO dimTime (start_time,hour,day,week,month,year,weekday)
SELECT
start_time,
EXTRACT(HOUR FROM start_time) AS hour,
EXTRACT(DAY FROM start_time) AS day,
EXTRACT(WEEK FROM start_time) AS week,
EXTRACT(MONTH FROM start_time) AS month,
EXTRACT(YEAR FROM start_time) AS year,
EXTRACT(WEEKDAY FROM start_time) AS weekday
FROM songPlay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, time_table_drop, artist_table_drop, song_table_drop, user_table_drop, staging_songs_table_drop, staging_events_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
