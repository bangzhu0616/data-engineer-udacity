# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id int primary key,
    start_time timestamp,
    user_id int,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE users (
	user_id int primary key,
	first_name varchar,
	last_name varchar,
	gender varchar,
	level varchar
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar primary key,
    title varchar,
    artist_id varchar,
    year int,
    duration decimal
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar primary key,
    name varchar,
    location varchar,
    latitude decimal,
    longitude decimal
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
ON conflict(songplay_id) do nothing;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s) 
ON conflict(user_id) do UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s) 
ON conflict(song_id) do nothing;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON conflict(artist_id) do nothing;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
SELECT song_id, artists.artist_id \
    FROM (songs JOIN artists ON artists.artist_id=songs.artist_id) \
    WHERE songs.title=%s \
    AND artists.name=%s \
    AND songs.duration=%s;
""")



# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]