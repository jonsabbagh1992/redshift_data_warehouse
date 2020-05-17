import configparser
import boto3
from aws_manager import AWSManager

# CONFIG
config = configparser.ConfigParser()
CONFIG_FILE = 'dwh.cfg'
config.read(CONFIG_FILE)
aws_manager = AWSManager()

REGION_NAME = aws_manager.get_region_name()
IAM_ROLE_NAME = aws_manager.get_cluster_iam_role()

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplay"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                 (artist varchar,
                                  auth varchar,
                                  firstName varchar,
                                  gender varchar,
                                  itemInSession int,
                                  lastName varchar,
                                  length float,
                                  level varchar,
                                  location varchar,
                                  method varchar,
                                  page varchar,
                                  registration bigint,
                                  sessionId int,
                                  song varchar,
                                  status int,
                                  ts bigint,
                                  userAgent varchar,
                                  userId int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                 (artist_id varchar NOT NULL,
                                  artist_latitude float,
                                  artist_location varchar,
                                  artist_longitude float,
                                  artist_name varchar NOT NULL,
                                  duration float,
                                  num_songs int,
                                  song_id varchar NOT NULL,
                                  title varchar NOT NULL,
                                  year int)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factSongplay (songplay_id int IDENTITY(0,1) PRIMARY KEY,
                                                                     start_time timestamp NOT NULL REFERENCES dimTime (start_time),
                                                                     user_id int NOT NULL REFERENCES dimUser (user_id) sortkey,
                                                                     level varchar,
                                                                     song_id varchar REFERENCES dimSong (song_id),
                                                                     artist_id varchar REFERENCES dimArtist (artist_id),
                                                                     session_id int,
                                                                     location varchar,
                                                                     user_agent varchar NOT NULL);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUser (user_id int PRIMARY KEY sortkey,
                                                            first_name varchar,
                                                            last_name varchar,
                                                            gender varchar,
                                                            level varchar)
                        diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSong (song_id varchar PRIMARY KEY sortkey,
                                                            title varchar NOT NULL,
                                                            artist_id varchar NOT NULL,
                                                            year int,
                                                            duration float)
                        diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtist (artist_id varchar PRIMARY KEY sortkey,
                                                                name varchar NOT NULL,
                                                                location varchar,
                                                                latitude float,
                                                                longitude float)
                         diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime (start_time timestamp PRIMARY KEY sortkey,
                                                            hour int NOT NULL,
                                                            day int NOT NULL,
                                                            week int NOT NULL,
                                                            month int NOT NULL,
                                                            year int NOT NULL,
                                                            weekday int NOT NULL);
""")

# STAGING TABLES

staging_events_copy = (f"""COPY staging_events FROM '{LOG_DATA}'
                           credentials 'aws_iam_role={IAM_ROLE_NAME}'
                           FORMAT AS json '{LOG_JSONPATH}'
                           compupdate off region '{REGION_NAME}';
""")

staging_songs_copy = (f""" COPY staging_songs FROM '{SONG_DATA}'
                           credentials 'aws_iam_role={IAM_ROLE_NAME}'
                           json 'auto'
                           compupdate off region '{REGION_NAME}';
""")

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO factSongplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT '1970-01-01'::date + e.ts/1000 * interval '1 second',
                                    e.userId,
                                    e.level,
                                    s.song_id,
                                    s.artist_id,
                                    e.sessionId,
                                    e.location,
                                    e.userAgent
                            FROM staging_events e
                            LEFT JOIN staging_songs s ON s.title = e.song
                            WHERE e.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO dimUser (user_id, first_name, last_name, gender, level)
                        WITH partitioned_data AS (
                            SELECT row_number() OVER (PARTITION by userid ORDER BY ts DESC),
                                    userid,
                                    firstName,
                                    lastName,
                                    gender,
                                    level,
                                    ts
                          FROM staging_events
                          WHERE userid IS NOT NULL
                        )
                        SELECT userid,
                               firstName,
                               lastName,
                               gender,
                               level
                        FROM partitioned_data
                        WHERE row_number = 1
""")

song_table_insert = ("""INSERT INTO dimSong (song_id, title, artist_id, year, duration)
                        SELECT song_id,
                               title,
                               artist_id,
                               year,
                               duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
""")


artist_table_insert = ("""INSERT INTO dimArtist (artist_id, name, location, latitude, longitude)
                          WITH partitioned_data AS (
                            SELECT row_number() OVER (PARTITION by artist_id),
                                   artist_id,
                                   artist_name,
                                   artist_location,
                                   artist_latitude,
                                   artist_longitude
                            FROM staging_songs
                            WHERE artist_id IS NOT NULL
                        )
                        SELECT artist_id,
                                   artist_name,
                                   artist_location,
                                   artist_latitude,
                                   artist_longitude
                        FROM partitioned_data
                        WHERE row_number = 1
""")

time_table_insert = ("""INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT('1970-01-01'::date + ts/1000 * interval '1 second')             AS start_time,
                               EXTRACT(hour FROM '1970-01-01'::date + ts/1000 * interval '1 second')    AS hour,
                               EXTRACT(day FROM '1970-01-01'::date + ts/1000 * interval '1 second')     AS day,
                               EXTRACT(week FROM '1970-01-01'::date + ts/1000 * interval '1 second')    AS week,
                               EXTRACT(month FROM '1970-01-01'::date + ts/1000 * interval '1 second')   AS month,
                               EXTRACT(year FROM '1970-01-01'::date + ts/1000 * interval '1 second')    AS year,
                               EXTRACT(weekday FROM '1970-01-01'::date + ts/1000 * interval '1 second') AS weekday
                        FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]