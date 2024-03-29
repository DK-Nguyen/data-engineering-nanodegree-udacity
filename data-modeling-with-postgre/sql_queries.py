# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# songplays: records in log data associated with song plays i.e. records with page NextSong
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id serial PRIMARY KEY , 
                                start_time timestamp without time zone  NOT NULL, 
                                user_id int NOT NULL, 
                                level varchar, 
                                song_id varchar, 
                                artist_id varchar, 
                                session_id varchar, 
                                location varchar, 
                                user_agent varchar); 
""")

# users: users in the app
user_table_create = ("""CREATE TABLE IF NOT EXISTS users ( 
                            user_id int PRIMARY KEY, 
                            first_name varchar NOT NULL, 
                            last_name varchar NOT NULL, 
                            gender varchar, 
                            level varchar); 
""")

# songs: songs in music database
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( 
                            song_id varchar PRIMARY KEY, 
                            title varchar NOT NULL, 
                            artist_id varchar NOT NULL, 
                            year int, 
                            duration float); 
""")

# artists: artists in music database
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists ( 
                            artist_id varchar PRIMARY KEY, 
                            name varchar NOT NULL, 
                            location varchar NOT NULL, 
                            lattitude float, 
                            longitude float); 
""")

# time: timestamps of records in songplays broken down into specific units
time_table_create = ("""CREATE TABLE IF NOT EXISTS time ( 
                            start_time time PRIMARY KEY, 
                            hour int, 
                            day int, 
                            week int, 
                            month int, 
                            year int, 
                            weekday int); 
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (songplay_id) DO UPDATE SET songplay_id = EXCLUDED.songplay_id
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, songs.artist_id FROM songs LEFT JOIN artists ON (songs.artist_id = artists.artist_id) WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
""") # The LEFT JOIN keyword returns all records from the left table (table1), and the matched records from the right table (table2).

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]