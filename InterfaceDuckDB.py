#!/usr/bin/python3

import duckdb
import psycopg2
import pandas as pd

DATABASE_NAME = 'dds_assgn1'
RANGE_PREFIX = 'range_part'
RROBIN_PREFIX = 'rrobin_part'

def getopenconnection(user='postgres', password='1234', dbname=DATABASE_NAME, host='localhost'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host
    )

def loadratings(tablename, ratingsfilepath, conn):
    # Đọc file vào DuckDB DataFrame
    con_duck = duckdb.connect()
    df = con_duck.execute(f"""
        SELECT 
            CAST(split_part(line, '::', 1) AS INTEGER) AS userid,
            CAST(split_part(line, '::', 2) AS INTEGER) AS movieid,
            CAST(split_part(line, '::', 3) AS FLOAT) AS rating
        FROM read_csv_auto('{ratingsfilepath}', delim='\n', header=False) as t(line)
        WHERE line IS NOT NULL AND length(line) > 0
    """).df()
    # Lưu vào PostgreSQL
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {tablename};")
    cur.execute(f"CREATE TABLE {tablename} (userid INTEGER, movieid INTEGER, rating FLOAT);")
    for row in df.itertuples(index=False):
        cur.execute(f"INSERT INTO {tablename} (userid, movieid, rating) VALUES (%s, %s, %s);", row)
    conn.commit()
    cur.close()
    con_duck.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(numberofpartitions):
        table_name = f"{RANGE_PREFIX}{i}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
    interval = 5.0 / numberofpartitions
    # Partition đầu tiên: 0 <= r <= interval
    cur.execute(f"""
        INSERT INTO {RANGE_PREFIX}0
        SELECT userid, movieid, rating FROM {ratingstablename}
        WHERE rating >= 0 AND rating <= {interval};
    """)
    # Các partition còn lại: lower < r <= upper
    for i in range(1, numberofpartitions):
        lower = i * interval
        upper = (i + 1) * interval
        cur.execute(f"""
            INSERT INTO {RANGE_PREFIX}{i}
            SELECT userid, movieid, rating FROM {ratingstablename}
            WHERE rating > {lower} AND rating <= {upper};
        """)
    openconnection.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_PREFIX}{i}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
    cur.execute(f"""
        CREATE TEMP VIEW temp_with_row_number AS
        SELECT userid, movieid, rating, row_number() OVER () as rn
        FROM {ratingstablename};
    """)
    for i in range(numberofpartitions):
        cur.execute(f"""
            INSERT INTO {RROBIN_PREFIX}{i} (userid, movieid, rating)
            SELECT userid, movieid, rating 
            FROM temp_with_row_number
            WHERE (rn - 1) % {numberofpartitions} = {i};
        """)
    cur.execute("DROP VIEW temp_with_row_number;")
    openconnection.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, movieid, rating))
    cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '{RANGE_PREFIX}%' AND table_schema='public';")
    num_partitions = cur.fetchone()[0]
    interval = 5.0 / num_partitions
    if rating >= 0 and rating <= interval:
        partition_index = 0
    else:
        partition_index = int((rating - 1e-8) // interval)
        if partition_index >= num_partitions:
            partition_index = num_partitions - 1
    cur.execute(f"INSERT INTO {RANGE_PREFIX}{partition_index} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, movieid, rating))
    openconnection.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, movieid, rating))
    cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '{RROBIN_PREFIX}%' AND table_schema='public';")
    num_partitions = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    partition_index = (total_rows - 1) % num_partitions
    cur.execute(f"INSERT INTO {RROBIN_PREFIX}{partition_index} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, movieid, rating))
    openconnection.commit()
    cur.close()

def create_db(dbname):
    import psycopg2
    con = psycopg2.connect(dbname='postgres', user='postgres', password='1234')
    con.set_isolation_level(0)
    cur = con.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS {dbname}')
    cur.execute(f'CREATE DATABASE {dbname}')
    con.close()

def count_partitions(prefix, openconnection):
    cur = openconnection.cursor()
    cur.execute(f'''
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name LIKE '{prefix}%';
    ''')
    count = cur.fetchone()[0]
    cur.close()
    return count
