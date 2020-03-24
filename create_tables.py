#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Handle Postgress interactions


@author: udacity, ucaiado

Created on 03/23/2020
"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Creates and connects to the sparkifydb

    :returns (cur, conn) objects. The connection and cursor to sparkifydb
    """

    # connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.

    :params cur: psycopg2 object. Cursor to execute queries
    :param conn:  psycopg2 object. Connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.

    :params cur: psycopg2 object. Cursor to execute queries
    :param conn:  psycopg2 object. Connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    cur, conn = create_database()

    print('drop tables...')
    drop_tables(cur, conn)
    print('create tables...')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
