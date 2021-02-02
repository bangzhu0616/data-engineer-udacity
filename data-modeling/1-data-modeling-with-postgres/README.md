### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

This project create a database and an ETL pipline for song play analysis. 

### How to run

1. run `create_tables.py`

```
$ python3 create_tables.py
```

2. run `etl.py`

```
$ python3 etl.py
```

3. run `test.ipynb` in jupyter notebook or jupyter-lab to test the etl.

### Tables

#### Fact Table

1. `songplays` - records in log data associated with song plays i.e. records with page `NextSong`

	* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

1. `users` - users in the app

	* user_id, first_name, last_name, gender, level

2. `songs` - songs in music database

	* song_id, title, artist_id, year, duration

3. `artists` - artists in music database

	* artist_id, name, location, latitude, longitude

4. `time` - timestamps of records in songplays broken down into specific units

	* start_time, hour, day, week, month, year, weekday


### Files

1. `test.ipynb` displays the first few rows of each table to check database.
2. `create_tables.py` drops and creates tables. Run this file to reset tables before each time of running ETL scripts.
3. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from song_data and log_data and loads them into tables. You can fill this out based on work in the ETL notebook.
5. `sql_queries.py` contains all sql queries, and is imported into the last three files above.
6. `README.md` provides discussion.