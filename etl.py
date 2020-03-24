#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Perform data etl in the data in the data folder


@author: udacity, ucaiado

Created on 03/23/2020
"""


import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
from sql_queries import *


'''
Begin help functions
'''


def addapt_numpy_float64(numpy_float64):
    '''Make numpy float works in psycopg2'''
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    '''Make numpy int works in psycopg2'''
    return AsIs(numpy_int64)

'''
End help functions
'''


def process_song_file(cur, filepath):
    '''
    Include data from filepath to songs and artits databases

    :params cur: psycopg2 object. Cursor to execute queries
    :param filepath:  string. Path to the data file
    '''

    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.where(pd.notnull(df), None)

    # insert song record
    song_data = list(df.iloc[0].loc[[
        'song_id', 'title', 'artist_id', 'year', 'duration']
    ].values)
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df.iloc[0].loc[
        ['artist_id', 'artist_name', 'artist_location', 'artist_latitude',
         'artist_longitude']
    ].values)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Include data from filepath to times, users and join this data to other
    infos to create the songplays fact table

    :params cur: psycopg2 object. Cursor to execute queries
    :param filepath:  string. Path to the data file
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = df.where(pd.notnull(df), None)

    # filter by NextSong action
    df = df[df.page.isin(['NextSong'])]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month,
                 t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month',
                     'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row['ts'],
            row['userId'],
            row['level'],
            songid,
            artistid,
            row['sessionId'],
            row['location'],
            row['userAgent']
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Create the dimension and fact tables based on the data in the files passed

    :params cur: psycopg2 object. Cursor to execute queries
    :param conn:  psycopg2 object. Connection to the database
    :param filepath:  string. Path to the data file
    :param func: function object. Function to use to process data
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('\n{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('...{}/{} files processed.'.format(i, num_files))


def main():
    # adapt numpy types to psycopg2
    register_adapter(np.float64, addapt_numpy_float64)
    register_adapter(np.int64, addapt_numpy_int64)

    # connect to database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # run etl
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
