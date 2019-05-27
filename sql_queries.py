import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
 

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession BIGINT,
lastName VARCHAR,
length DECIMAL,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration BIGINT,
sessionId BIGINT,
song VARCHAR,
status BIGINT,
ts BIGINT,
userAgent VARCHAR,
userId BIGINT
)
DISTKEY(artist)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INT,
artist_id VARCHAR,
artist_lattitude VARCHAR,
artist_longitude VARCHAR,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration DECIMAL,
year INT
)
DISTKEY(artist_name)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id INT IDENTITY(0,1),
start_time VARCHAR,
user_id INT REFERENCES users(user_id),
level VARCHAR,
song_id VARCHAR REFERENCES songs(song_id),
artist_id VARCHAR REFERENCES artists(artist_id),
session_id INT,
location VARCHAR,
user_agent VARCHAR
)
DISTKEY(level)
COMPOUND SORTKEY(song_id, artist_id)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id INT PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
)
DISTKEY(gender)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR REFERENCES artists(artist_id),
year INT,
duration DECIMAL
)
DISTKEY(artist_id)
SORTKEY(year)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
lattitude VARCHAR,
longitude VARCHAR
)
DISTKEY(location)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time VARCHAR,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
)
DISTKEY(year)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {s3_bucket}
iam_role '{arn}' 
region 'us-west-2' 
FORMAT AS JSON {json_path};
""").format(s3_bucket = config['S3']['LOG_DATA'], arn = config['IAM_ROLE']['ARN'],
            json_path = config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM {s3_bucket}
iam_role '{arn}'
region 'us-west-2'
FORMAT AS JSON 'auto'
""").format(s3_bucket = config['S3']['SONG_DATA'], arn = config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = """INSERT INTO songplays (session_id, start_time, user_id, level, song_id, artist_id, location, user_agent)
SELECT 
DISTINCT e.sessionId, 
to_char(TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ', 'HH24:MI:SS'), 
e.userId, e.level, s.song_id, s.artist_id, e.location, e.userAgent
FROM staging_events as e
INNER JOIN staging_songs as s
ON e.artist = s.artist_name
WHERE e.page = 'NextSong'
"""

user_table_insert = ("""INSERT INTO users (
SELECT 
DISTINCT userId,
firstName,
lastName,
gender,
level
FROM staging_events
WHERE userId IS NOT NULL)""")

song_table_insert = ("""INSERT INTO songs (
SELECT 
DISTINCT song_id,
title,
artist_id,
year,
duration
FROM staging_songs
WHERE song_id IS NOT NULL
AND artist_id IS NOT NULL)""")

artist_table_insert = ( """INSERT INTO artists (
SELECT 
DISTINCT artist_id,
artist_name,
artist_location,
artist_lattitude,
artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL)""")

time_table_insert = ("""INSERT INTO time 
(
SELECT
to_char(TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ', 'HH24:MI:SS'),
extract(hr from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(day from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(w from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(mon from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(y from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(weekday from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second '))
FROM staging_events
)""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, artist_table_insert, user_table_insert, song_table_insert, time_table_insert]