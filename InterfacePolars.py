import polars as pl
import psycopg2
import math

DATABASE_NAME = 'dds_assgn1'

RANGE_PREFIX = 'range_part'
RROBIN_PREFIX = 'rrobin_part'

def getopenconnection(user='postgres', password='123456', dbname='dds_assgn1'):
    return psycopg2.connect(
        database=dbname,
        user=user,
        password=password,
        host='localhost',
        port='5432'
    )


def save_partition_to_postgres(partitions, conn):
    cur = conn.cursor()
    for table_name, part_df in partitions.items():
        # Chuyển polars -> pandas
        pdf = part_df.to_pandas()

        # Tạo bảng nếu chưa có
        cur.execute(f'''
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        ''')
        conn.commit()

        # Sử dụng COPY để insert hiệu quả
        from io import StringIO
        buffer = StringIO()
        pdf.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cur.copy_from(buffer, table_name, sep=",")
        conn.commit()

    cur.close()
    

def loadratings(tablename, ratingsfilepath, conn):
    """Load ratings from file into database table"""
    cur = conn.cursor()
    
    # Create the main ratings table
    cur.execute(f'''
        DROP TABLE IF EXISTS {tablename};
        CREATE TABLE {tablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        );
    ''')
    
    # Read and parse the file
    with open(ratingsfilepath, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split("::")
            if len(parts) >= 4:  # Handle cases with more than 4 parts
                user_id, movie_id, rating, timestamp = parts[0], parts[1], parts[2], parts[3]
                cur.execute(f'''
                    INSERT INTO {tablename} (userid, movieid, rating)
                    VALUES (%s, %s, %s);
                ''', (int(user_id), int(movie_id), float(rating)))
    
    conn.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    if numberofpartitions <= 0:
        raise ValueError("Number of partitions must be greater than 0")
    for i in range(numberofpartitions):
        table_name = f"{RANGE_PREFIX}{i}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)
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
    if numberofpartitions <= 0:
        raise ValueError("Number of partitions must be greater than 0")
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_PREFIX}{i}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)
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
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))
    cur.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name LIKE '{RANGE_PREFIX}%' AND table_schema='public';
    """)
    num_partitions = cur.fetchone()[0]
    interval = 5.0 / num_partitions
    if rating >= 0 and rating <= interval:
        partition_index = 0
    else:
        partition_index = int((rating - 1e-8) // interval)
        if partition_index >= num_partitions:
            partition_index = num_partitions - 1
    cur.execute(f"""
        INSERT INTO {RANGE_PREFIX}{partition_index} (userid, movieid, rating)
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))
    openconnection.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))
    cur.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name LIKE '{RROBIN_PREFIX}%' AND table_schema='public';
    """)
    num_partitions = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    partition_index = (total_rows - 1) % num_partitions
    cur.execute(f"""
        INSERT INTO {RROBIN_PREFIX}{partition_index} (userid, movieid, rating)
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))
    openconnection.commit()
    cur.close()


def create_db(dbname):
    import psycopg2
    import os
    con = psycopg2.connect(dbname='postgres', user='postgres', password='123456')
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