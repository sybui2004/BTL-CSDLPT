import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import logging
import mmap
import os
import multiprocessing as mp
from gc import disable as gc_disable, enable as gc_enable
import sys
from typing import List, Tuple, Dict, Optional

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='postgres', password='1234', dbname=DATABASE_NAME):
    """Get an open connection to the database."""
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host='localhost',
            password=password
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise

def get_file_chunks(
    file_name: str, 
    max_cpu: int = 8
) -> tuple[int, list[tuple[str, int, int]]]:
    """Split file into chunks for parallel processing."""
    cpu_count = min(max_cpu, mp.cpu_count())
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    start_end = []
    with open(file_name, mode="rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            def is_new_line(position):
                if position == 0:
                    return True
                else:
                    return mm[position-1:position] == b"\n"

            def next_line(position):
                while position < file_size and mm[position:position+1] != b"\n":
                    position += 1
                return position + 1 if position < file_size else file_size

            chunk_start = 0
            while chunk_start < file_size:
                chunk_end = min(file_size, chunk_start + chunk_size)

                while chunk_end < file_size and not is_new_line(chunk_end):
                    chunk_end -= 1

                if chunk_start == chunk_end:
                    chunk_end = next_line(chunk_end)

                start_end.append((file_name, chunk_start, chunk_end))
                chunk_start = chunk_end

    return cpu_count, start_end

def process_ratings_chunk(
    file_name: str, 
    chunk_start: int, 
    chunk_end: int
) -> list[tuple[int, int, float]]:
    """Process a chunk of the ratings file using memory mapping."""
    results = []
    
    with open(file_name, mode="rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            start_pos = chunk_start
            if chunk_start != 0:
                # Find the next newline after chunk_start
                while start_pos < len(mm) and mm[start_pos:start_pos+1] != b'\n':
                    start_pos += 1
                if start_pos < len(mm):
                    start_pos += 1  # Move past the newline
            
            # Adjust end position to end of line
            end_pos = chunk_end
            if end_pos < len(mm):
                while end_pos < len(mm) and mm[end_pos:end_pos+1] != b'\n':
                    end_pos += 1
                if end_pos < len(mm):
                    end_pos += 1  # Include the newline
            
            # Process the chunk
            current_pos = start_pos
            while current_pos < end_pos and current_pos < len(mm):
                # Find end of current line
                line_end = current_pos
                while line_end < end_pos and line_end < len(mm) and mm[line_end:line_end+1] != b'\n':
                    line_end += 1
                
                if line_end > current_pos:
                    # Extract line
                    line_bytes = mm[current_pos:line_end]
                    try:
                        line = line_bytes.decode('utf-8').strip()
                        parts = line.split("::")
                        if len(parts) >= 3:
                            userid = int(parts[0])
                            movieid = int(parts[1])
                            rating = float(parts[2])
                            results.append((userid, movieid, rating))
                    except (UnicodeDecodeError, ValueError):
                        # Skip malformed lines
                        pass
                
                current_pos = line_end + 1
    return results

def loadratings(ratingstablename: str, ratingsfilepath: str, openconnection):
    import tempfile
    import io
    
    con = openconnection
    cur = con.cursor()
    try:
        # Drop and create table (no indexes yet for faster loading)
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {ratingstablename} (
                userid INTEGER NOT NULL,
                movieid INTEGER NOT NULL,
                rating FLOAT NOT NULL
            );
        """)
        
        # Use COPY FROM for maximum speed
        print("Processing file for bulk load...")
        
        # Create temporary file in CSV format for COPY
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            temp_filename = temp_file.name
            
            # Process chunks in parallel and write to temp file
            cpu_count, chunks = get_file_chunks(ratingsfilepath, max_cpu=mp.cpu_count())
            
            with mp.Pool(cpu_count) as pool:
                chunk_results = pool.starmap(process_ratings_chunk, chunks)
            
            # Write all results to temp CSV file
            total_records = 0
            for chunk_data in chunk_results:
                if chunk_data:
                    for userid, movieid, rating in chunk_data:
                        temp_file.write(f"{userid},{movieid},{rating}\n")
                        total_records += 1
                        if total_records % 500000 == 0:
                            print(f"Processed {total_records} records...")
        
        print(f"Loading {total_records} records using COPY...")
        
        # Use PostgreSQL COPY for ultra-fast loading
        with open(temp_filename, 'r') as f:
            cur.copy_expert(
                f"COPY {ratingstablename} (userid, movieid, rating) FROM STDIN WITH CSV",
                f
            )
        
        # Clean up temp file
        os.unlink(temp_filename)
        
        # Add indexes AFTER loading data (much faster)
        print("Creating indexes...")
        cur.execute(f"CREATE INDEX idx_{ratingstablename}_userid ON {ratingstablename}(userid);")
        cur.execute(f"CREATE INDEX idx_{ratingstablename}_movieid ON {ratingstablename}(movieid);")
        cur.execute(f"CREATE INDEX idx_{ratingstablename}_rating ON {ratingstablename}(rating);")
        
        # Add constraints after loading
        cur.execute(f"ALTER TABLE {ratingstablename} ADD CHECK (rating >= 0 AND rating <= 5.0);")
        
        openconnection.commit()
        print(f"Successfully loaded {total_records} records")
        
    except Exception as e:
        openconnection.rollback()
        # Clean up temp file on error
        try:
            if 'temp_filename' in locals():
                os.unlink(temp_filename)
        except:
            pass
        raise

# def loadratings(ratingstablename: str, ratingsfilepath: str, openconnection):
#     """Ultra-fast loading using direct COPY FROM STDIN without temp file."""
#     import io
    
#     con = openconnection
#     cur = con.cursor()
#     try:
#         # Drop and create table (no indexes/constraints yet)
#         cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE;")
#         cur.execute(f"""
#             CREATE TABLE {ratingstablename} (
#                 userid INTEGER NOT NULL,
#                 movieid INTEGER NOT NULL,
#                 rating FLOAT NOT NULL
#             );
#         """)
        
#         print("Processing file for ultra-fast bulk load...")
        
#         # Process chunks in parallel
#         cpu_count, chunks = get_file_chunks(ratingsfilepath, max_cpu=mp.cpu_count())
        
#         with mp.Pool(cpu_count) as pool:
#             chunk_results = pool.starmap(process_ratings_chunk, chunks)
        
#         # Create in-memory CSV buffer
#         csv_buffer = io.StringIO()
#         total_records = 0
        
#         for chunk_data in chunk_results:
#             if chunk_data:
#                 for userid, movieid, rating in chunk_data:
#                     csv_buffer.write(f"{userid},{movieid},{rating}\n")
#                     total_records += 1
#                     if total_records % 500000 == 0:
#                         print(f"Processed {total_records} records...")
        
#         print(f"Loading {total_records} records using COPY FROM STDIN...")
        
#         # Reset buffer position to beginning
#         csv_buffer.seek(0)
        
#         # Use COPY FROM STDIN with in-memory buffer
#         cur.copy_expert(
#             f"COPY {ratingstablename} (userid, movieid, rating) FROM STDIN WITH CSV",
#             csv_buffer
#         )
        
#         # Add indexes AFTER loading data (much faster)
#         print("Creating indexes...")
#         cur.execute(f"CREATE INDEX idx_{ratingstablename}_userid ON {ratingstablename}(userid);")
#         cur.execute(f"CREATE INDEX idx_{ratingstablename}_movieid ON {ratingstablename}(movieid);")
#         cur.execute(f"CREATE INDEX idx_{ratingstablename}_rating ON {ratingstablename}(rating);")
        
#         # Add constraints after loading
#         cur.execute(f"ALTER TABLE {ratingstablename} ADD CHECK (rating >= 0 AND rating <= 5.0);")
        
#         openconnection.commit()
#         print(f"Successfully loaded {total_records} records")
        
#     except Exception as e:
#         openconnection.rollback()
#         raise
#     finally:
#         csv_buffer.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    delta = 5 / numberofpartitions
    prefix = "range_part"

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

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    cur.execute("SET work_mem = '128MB';")
    create_queries = ["CREATE UNLOGGED TABLE " + RROBIN_TABLE_PREFIX + str(i) + " (userid INTEGER, movieid INTEGER, rating FLOAT);" for i in range(numberofpartitions)]
    cur.execute(" ".join(create_queries))
    
    insert_queries = [
        "INSERT INTO " + RROBIN_TABLE_PREFIX + str(i) + " (userid, movieid, rating) SELECT userid, movieid, rating FROM (" +
        "SELECT userid, movieid, rating, ROW_NUMBER() OVER () - 1 AS rnum FROM " + ratingstablename +
        ") AS temp WHERE rnum % " + str(numberofpartitions) + " = " + str(i) + ";"
        for i in range(numberofpartitions)
    ]
    cur.execute(" ".join(insert_queries))
    
    con.commit()
    cur.close()

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

def create_db(dbname=DATABASE_NAME):
    """Create database with error handling."""
    try:
        con = getopenconnection(dbname='postgres')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(
            'SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s',
            (dbname,)
        )
        count = cur.fetchone()[0]
        
        if count == 0:
            cur.execute(f'CREATE DATABASE {dbname}')
            print(f"Database {dbname} created successfully")
        else:
            print(f"Database {dbname} already exists")
    except Exception as e:
        print(f"Error creating database: {e}")
        raise

def count_partitions(prefix: str, openconnection) -> int:
    """Count partitions with given prefix."""
    con = openconnection
    cur = con.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s AND table_type = 'BASE TABLE';",
        (f'{prefix}%',)
    )
    return cur.fetchone()[0]