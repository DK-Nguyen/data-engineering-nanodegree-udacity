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
# Note that Amazon Redshift does not enforce unique, primary-key, and foreign-key constraints.

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
        event_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
        artist VARCHAR(255),
        auth VARCHAR(100),
        user_first_name VARCHAR(255),
        user_gender VARCHAR(10),
        item_in_section INTEGER,
        user_last_name VARCHAR(255),
        song_length DOUBLE PRECISION,
        user_level VARCHAR(50),
        location VARCHAR(255),
        method VARCHAR(10),
        page VARCHAR(50),
        registration VARCHAR(255),
        session_id INTEGER,
        song_title VARCHAR(255),
        status INTEGER, 
        ts BIGINT,
        user_agent VARCHAR(255),
        user_id INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR(255) NOT NULL PRIMARY KEY,
        num_songs INTEGER,
        artist_id VARCHAR(100),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        title VARCHAR(255),
        duration DOUBLE PRECISION,
        year INTEGER
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender VARCHAR(50),
        level VARCHAR(100)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(255) NOT NULL PRIMARY KEY,
        title VARCHAR(255),
        artist_id VARCHAR(255),
        year INTEGER,
        duration DOUBLE PRECISION
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(255) NOT NULL PRIMARY KEY,
        name VARCHAR(255),
        location VARCHAR(255),
        lattitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
)
""")

# fact table

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
        songplays INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time (start_time),
        user_id INTEGER REFERENCES users (user_id),
        level VARCHAR(100),
        song_id VARCHAR(255) REFERENCES songs (song_id),
        artist_id VARCHAR(255) REFERENCES artists (artist_id),
        session_id VARCHAR(255),
        location VARCHAR(255),
        user_agent VARCHAR(255)
)
""")

# COPY DATA INTO STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2' 
FORMAT AS JSON {}
COMPUPDATE OFF;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto'
REGION 'us-west-2'
COMPUPDATE OFF;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, 
                       artist_id, session_id, location, user_agent)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
    e.user_id,
    e.user_level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM  staging_events e, staging_songs s
WHERE e.page = 'NextSong'
AND   e.song_title = s.title
AND   e.song_length = s.duration
AND   e.artist = s.artist_name
AND user_id NOT IN (
SELECT DISTINCT s.user_id FROM songplays s 
WHERE s.user_id = user_id
AND s.start_time = start_time
AND s.session_id = session_id )
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id,
    user_first_name,
    user_last_name,
    user_gender,
    user_level
FROM staging_events
WHERE staging_events.page = 'NextSong'
AND user_id NOT IN (SELECT DISTINCT user_id FROM users) -- do this because in RedShift, Primary Key constraint is not enforced
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT 
    start_time, 
    EXTRACT(hr from start_time) AS hour,
    EXTRACT(d from start_time) AS day,
    EXTRACT(w from start_time) AS week,
    EXTRACT(mon from start_time) AS month,
    EXTRACT(yr from start_time) AS year, 
    EXTRACT(weekday from start_time) AS weekday 
FROM (
SELECT DISTINCT  TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time 
FROM staging_events e
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]