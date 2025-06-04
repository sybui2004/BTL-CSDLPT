#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import pandas as pd
import psycopg2.extras
import multiprocessing as mp
from io import StringIO
import time
import functools

DATABASE_NAME = 'dds_assgn1'

def measure_time(func):
    @functools.wraps(func)
    def wrapper_measure_time(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"Hàm '{func.__name__}' thực thi trong {(end - start):.4f} giây.")
        return result
    return wrapper_measure_time

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# def loadratings(ratingstablename, ratingsfilepath, openconnection): 
#     import multiprocessing as mp
#     import os
#     from functools import partial
    
#     def process_chunk(chunk_data, table_name, db_config):
#         """Xử lý một chunk của file"""
#         import psycopg2
#         from io import StringIO
        
#         # Tạo connection riêng cho worker
#         worker_con = psycopg2.connect(**db_config)
#         worker_con.autocommit = False
#         cur = worker_con.cursor()
        
#         try:
#             processed_data = StringIO()
#             valid_count = 0
            
#             for line in chunk_data:
#                 line = line.strip()
#                 if line:
#                     parts = line.split(':')
#                     if len(parts) >= 5:
#                         try:
#                             userid = int(parts[0])
#                             movieid = int(parts[2])
#                             rating = float(parts[4])
                            
#                             if 0 <= rating <= 5:
#                                 processed_data.write(f"{userid}\t{movieid}\t{rating}\n")
#                                 valid_count += 1
#                         except (ValueError, IndexError):
#                             continue
            
#             processed_data.seek(0)
#             if valid_count > 0:
#                 cur.copy_from(processed_data, table_name, sep='\t', 
#                              columns=('userid', 'movieid', 'rating'))
#                 worker_con.commit()
            
#             return valid_count
            
#         except Exception as e:
#             worker_con.rollback()
#             raise e
#         finally:
#             cur.close()
#             worker_con.close()
    
#     # Main function
#     create_db(DATABASE_NAME)
#     con = openconnection
#     cur = con.cursor()
    
#     try:
#         # Setup table
#         cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE;")
#         cur.execute(f"""
#             CREATE UNLOGGED TABLE {ratingstablename}(
#                 userid INTEGER NOT NULL, 
#                 movieid INTEGER NOT NULL, 
#                 rating FLOAT NOT NULL
#             ) WITH (fillfactor=100);
#         """)
#         con.commit()
        
#         # Đọc file và chia thành chunks
#         file_size = os.path.getsize(ratingsfilepath)
#         chunk_size = file_size // num_workers
        
#         chunks = []
#         with open(ratingsfilepath, 'r', encoding='utf-8') as f:
#             current_chunk = []
#             for i, line in enumerate(f):
#                 current_chunk.append(line)
#                 if len(current_chunk) >= chunk_size // 100:  # Estimate lines per chunk
#                     chunks.append(current_chunk)
#                     current_chunk = []
            
#             if current_chunk:
#                 chunks.append(current_chunk)
        
#         # Database config cho workers
#         db_config = {
#             'host': 'localhost',
#             'database': DATABASE_NAME,
#             'user': 'postgres', 
#             'password': '1234'
#         }
        
#         # Process parallel
#         print(f"Xử lý parallel với {len(chunks)} chunks...")
#         with mp.Pool(num_workers) as pool:
#             process_func = partial(process_chunk, table_name=ratingstablename, db_config=db_config)
#             results = pool.map(process_func, chunks)
        
#         total_records = sum(results)
#         print(f"Parallel load hoàn thành! Tổng cộng {total_records} records")
        
#         # Tạo indexes
#         print("Tạo indexes...")
#         cur.execute(f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_userid ON {ratingstablename}(userid);")
#         cur.execute(f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_movieid ON {ratingstablename}(movieid);")  
#         cur.execute(f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_rating ON {ratingstablename}(rating);")
#         cur.execute(f"ANALYZE {ratingstablename};")
        
#         con.commit()
        
#     except Exception as e:
#         con.rollback()
#         raise e
#     finally:
#         cur.close()

@measure_time
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    from io import StringIO
    
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    
    try:
        # Chỉ set những parameter có thể thay đổi được
        cur.execute("SET synchronous_commit = OFF;")
        cur.execute("SET work_mem = '1024MB';")  # Tăng memory cho sort/hash
        cur.execute("SET maintenance_work_mem = '2097151kB';")  # Tăng memory cho maintenance
        
        # Tạo bảng UNLOGGED (nhanh hơn nhiều, không write WAL)
        cur.execute(f"""
            CREATE UNLOGGED TABLE {ratingstablename}(
                userid INTEGER, 
                movieid INTEGER, 
                rating FLOAT
            ) WITH (fillfactor=100);
        """)
        
        # Xử lý file với buffer lớn
        processed_data = StringIO()
        
        with open(ratingsfilepath, 'r', encoding='utf-8', buffering=65536) as f:
            for line in f:
                parts = line.strip().split("::")
                if len(parts) >= 4:
                    processed_data.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
        
        processed_data.seek(0)
        
        # Copy với buffer size lớn
        cur.copy_from(processed_data, ratingstablename, sep='\t', size=65536)
        
        cur.close()
        con.commit()
        
    except Exception as e:
        con.rollback()
        cur.close()
        raise e

@measure_time
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    delta = 5 / numberofpartitions
    prefix = "range_part"
    cur.execute("SET synchronous_commit = OFF;")
    cur.execute("SET work_mem = '1024MB';")  # Tăng memory cho sort/hash
    cur.execute("SET maintenance_work_mem = '2097151kB';")  # Tăng memory cho maintenance
    for i in range(numberofpartitions):
        minR = i * delta
        maxR = minR + delta
        table = f"{prefix}{i}"
        
        # Create as fast as possible
        cur.execute(f"""
            CREATE UNLOGGED TABLE {table} AS
            SELECT userid, movieid, rating
            FROM {ratingstablename}
            WHERE rating {'>=' if i == 0 else '>'} {minR}
              AND rating <= {maxR};
        """)

    openconnection.commit()
    cur.close()


# def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
#     cur = openconnection.cursor()
#     RROBIN_TABLE_PREFIX = 'rrobin_part'

#     cur.execute("SET synchronous_commit = OFF;")
#     cur.execute("SET work_mem = '1024MB';")  # Tăng memory cho sort/hash
#     cur.execute("SET maintenance_work_mem = '2097151kB';")  # Tăng memory cho maintenance
#     for i in range(numberofpartitions):
#         table = RROBIN_TABLE_PREFIX + str(i)
#         query = f"""
#         CREATE UNLOGGED TABLE {table} AS
#         SELECT userid, movieid, rating FROM (
#             SELECT userid, movieid, rating, ROW_NUMBER() OVER () - 1 AS rnum FROM {ratingstablename}
#         ) AS temp
#         WHERE rnum % {numberofpartitions} = {i};
#         """
#         cur.execute(query)

#     openconnection.commit()
#     cur.close()
@measure_time
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    temp_table = "rrobin_temp"
    cur.execute("SET synchronous_commit = OFF;")
    cur.execute("SET work_mem = '1024MB';")  # Tăng memory cho sort/hash
    cur.execute("SET maintenance_work_mem = '2097151kB';")  # Tăng memory cho maintenance
    
    cur.execute(f"""
        CREATE UNLOGGED TABLE {temp_table} AS
        SELECT userid, movieid, rating,
               (ROW_NUMBER() OVER () - 1) AS rnum,
               (ROW_NUMBER() OVER () - 1) % {numberofpartitions} AS partition_id
        FROM {ratingstablename};
    """)

    # Bước 2: Tạo các bảng partition và chèn dữ liệu
    for i in range(numberofpartitions):
        part_table = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"DROP TABLE IF EXISTS {part_table};")
        cur.execute(f"""
            CREATE UNLOGGED TABLE {part_table} AS
            SELECT userid, movieid, rating FROM {temp_table} WHERE partition_id = {i};
        """)
    cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
    openconnection.commit()
    cur.close()

@measure_time
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("select count(*) from " + ratingstablename + ";");
    total_rows = (cur.fetchall())[0][0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows-1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

@measure_time
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

@measure_time
def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count

