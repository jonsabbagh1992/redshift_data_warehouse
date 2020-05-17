Sparkify Redshift Database
==========================

Sparkify (a startup) collects data on songs and user activity on their new music streaming app. We were particularly interested in understanding what songs users are listening to. This task was difficult as the data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This ETL pipeline processes the data from the JSON logs and loads them into a Redshift database using a star schema. 

Database Schema
===============

The following star schema was optimized to run queries on songs. Here are the details

Facts Table
-----------

1. **songplay**: records in log data associated with song plays i.e. records with page **NextSong**
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
-----------

2. **users**: users in the app
    - user_id, first_name, last_name, gender, level


3. **songs** : songs in music database
    - song_id, title, artist_id, year, duration


4. **artists**: artists in music database
    - artist_id, name, location, latitude, longitude


5. **time**:timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday
    
How to Run
=====================
You will need to have your own AWS account set up, with a user that has the following permissions:
    
    - IAMFullAccess
    
    - AmazonRedshiftFullAccess
    
    - AmazonS3ReadOnlyAccess

Once that is set-up, enter the key and secret access codes in **dwh.cfg**. Finally, run **run.py** to create the tables and load the data into Redshift.

Don't forget to run **tear_down_resources.py** once you are done.

Enjoy!