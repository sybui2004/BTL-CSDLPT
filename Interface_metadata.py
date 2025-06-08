#!/usr/bin/python3
#
# Interface for the assignment with metadata optimization
#

import psycopg2
import time
import functools

DATABASE_NAME = 'dds_assgn1'

RANGE_METADATA_TABLE = 'range_metadata'
RROBIN_METADATA_TABLE = 'rrobin_metadata'

def measure_time(func):
    @functools.wraps(func)
    def wrapper_measure_time(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"Hàm '{func.__name__}' thực thi trong {(end - start):.6f} giây.")
        return result
    return wrapper_measure_time

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    """
    Tạo kết nối đến PostgreSQL database
    """
    return psycopg2.connect(f"dbname='{dbname}' user='{user}' host='localhost' password='{password}'")

def init_range_metadata_table(openconnection):
    """
    Khởi tạo bảng metadata cho phân mảnh range để lưu trữ thông tin về:
    - Số lượng phân mảnh
    cho từng bảng gốc
    """
    cur = openconnection.cursor()
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {RANGE_METADATA_TABLE} (
            tablename VARCHAR(100) PRIMARY KEY,
            partition_count INTEGER NOT NULL
        );
    ''')
    openconnection.commit()
    cur.close()

def init_rrobin_metadata_table(openconnection):
    """
    Khởi tạo bảng metadata cho phân mảnh round robin để lưu trữ thông tin về:
    - Số lượng phân mảnh
    - Chỉ số của phân mảnh cuối cùng vừa được thêm dữ liệu
    cho từng bảng gốc
    """
    cur = openconnection.cursor()
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {RROBIN_METADATA_TABLE} (
            tablename VARCHAR(100) PRIMARY KEY,
            partition_count INTEGER NOT NULL,
            last_partition_index INTEGER NOT NULL
        );
    ''')
    openconnection.commit()
    cur.close()

def update_range_metadata(openconnection, tablename, partition_count):
    """
    Cập nhật metadata cho phân mảnh range để lưu lại số lượng phân mảnh cho bảng gốc tương ứng
    """
    cur = openconnection.cursor()
    cur.execute(f"INSERT INTO {RANGE_METADATA_TABLE} (tablename, partition_count) VALUES (%s, %s) ON CONFLICT (tablename) DO UPDATE SET partition_count = EXCLUDED.partition_count", (tablename, partition_count))
    openconnection.commit()
    cur.close()

def update_rrobin_metadata(openconnection, tablename, partition_count, last_partition_index):
    """
    Cập nhật metadata cho phân mảnh round robin để lưu lại:
    - Số lượng phân mảnh
    - Chỉ số phân mảnh cuối cùng vừa được thêm dữ liệu
    cho bảng gốc tương ứng
    """
    cur = openconnection.cursor()
    cur.execute(f"INSERT INTO {RROBIN_METADATA_TABLE} (tablename, partition_count, last_partition_index) VALUES (%s, %s, %s) ON CONFLICT (tablename) DO UPDATE SET partition_count = EXCLUDED.partition_count, last_partition_index = EXCLUDED.last_partition_index", (tablename, partition_count, last_partition_index))
    openconnection.commit()
    cur.close()

def get_range_metadata(openconnection, tablename):
    """
    Lấy số lượng phân mảnh cho bảng gốc tương ứng từ bảng metadata
    """
    cur = openconnection.cursor()
    cur.execute(f"SELECT partition_count FROM {RANGE_METADATA_TABLE} WHERE tablename = %s", (tablename,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0

def get_rrobin_metadata(openconnection, tablename):
    """
    Lấy số lượng phân mảnh và chỉ số phân mảnh cuối cùng cho bảng gốc tương ứng từ bảng metadata
    """
    cur = openconnection.cursor()
    cur.execute(f"SELECT partition_count, last_partition_index FROM {RROBIN_METADATA_TABLE} WHERE tablename = %s", (tablename,))
    row = cur.fetchone()
    cur.close()
    if row:
        return row[0], row[1]
    return 0, -1

@measure_time
def loadratings(ratingstablename: str, ratingsfilepath: str, openconnection):
    """
    Load dữ liệu ratings từ file vào database
    """
    from io import StringIO

    # Tạo database mới nếu chưa tồn tại
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()

    try:
        # Tắt synchronous_commit để tăng tốc độ ghi dữ liệu
        cur.execute("SET synchronous_commit = OFF;")

        # Tăng memory cho sort/hash
        cur.execute("SET work_mem = '1024MB';")

        # Tăng memory cho maintenance
        cur.execute("SET maintenance_work_mem = '2097151kB';")

        # Tạo bảng UNLOGGED (nhanh hơn nhiều, không write WAL)
        cur.execute(f"""
            CREATE TABLE {ratingstablename}(
                userid INTEGER, 
                movieid INTEGER, 
                rating FLOAT
            ) WITH (fillfactor=100);
        """)

        processed_data = StringIO()
        with open(ratingsfilepath, 'r', encoding='utf-8', buffering=65536) as f:
            for line in f:
                parts = line.strip().split("::")
                if len(parts) >= 4:
                    processed_data.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")

        processed_data.seek(0)
        cur.copy_from(processed_data, ratingstablename, sep='\t', size=65536)
        con.commit()

    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()

@measure_time
def rangepartition(ratingstablename: str, numberofpartitions: int, openconnection):
    cur = openconnection.cursor()
    prefix = "range_part"
    try:
        init_range_metadata_table(openconnection)
        cur.execute("SET synchronous_commit = OFF;")
        cur.execute("SET work_mem = '1024MB';")
        cur.execute("SET maintenance_work_mem = '2097151kB';")
        delta = 5.0 / numberofpartitions
        for i in range(numberofpartitions):
            minR = i * delta
            maxR = minR + delta
            table = f"{prefix}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {table};")
            cur.execute(f"""
                CREATE TABLE {table} AS
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating {'>=' if i == 0 else '>'} {minR}
                  AND rating <= {maxR};
            """)
        update_range_metadata(openconnection, ratingstablename, numberofpartitions)
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        cur.close()

@measure_time
def roundrobinpartition(ratingstablename: str, numberofpartitions: int, openconnection):
    cur = openconnection.cursor()
    prefix = 'rrobin_part'
    temp_table = "rrobin_temp"
    try:
        init_rrobin_metadata_table(openconnection)
        cur.execute("SET synchronous_commit = OFF;")
        cur.execute("SET work_mem = '1024MB';")
        cur.execute("SET maintenance_work_mem = '2097151kB';")
        cur.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT userid, movieid, rating,
                   (ROW_NUMBER() OVER () - 1) AS rnum,
                   (ROW_NUMBER() OVER () - 1) % {numberofpartitions} AS partition_id
            FROM {ratingstablename};
        """)
        for i in range(numberofpartitions):
            part_table = f"{prefix}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {part_table};")
            cur.execute(f"""
                CREATE TABLE {part_table} AS
                SELECT userid, movieid, rating 
                FROM {temp_table} 
                WHERE partition_id = {i};
            """)
        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cur.fetchone()[0]
        last_partition_index = (total_rows - 1) % numberofpartitions if total_rows > 0 else -1
        update_rrobin_metadata(openconnection, ratingstablename, numberofpartitions, last_partition_index)
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        cur.close()

@measure_time
def roundrobininsert(ratingstablename: str, userid: int, itemid: int, rating: float, openconnection):
    con = openconnection
    cur = con.cursor()
    prefix = 'rrobin_part'
    try:
        partition_count, last_partition_index = get_rrobin_metadata(con, ratingstablename)
        if partition_count == 0:
            raise Exception('Chưa phân mảnh round robin cho bảng này!')
        
        partition_index = (last_partition_index + 1) % partition_count
        table_name = f"{prefix}{partition_index}"
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        update_rrobin_metadata(con, ratingstablename, partition_count, partition_index)
        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()

@measure_time
def rangeinsert(ratingstablename: str, userid: int, itemid: int, rating: float, openconnection):
    con = openconnection
    cur = con.cursor()
    prefix = 'range_part'
    try:
        partition_count = get_range_metadata(con, ratingstablename)
        if partition_count == 0:
            raise Exception('Chưa phân mảnh range cho bảng này!')
        
        delta = 5.0 / partition_count
        index = int(rating / delta)
        if rating % delta == 0 and index != 0:
            index = index - 1
        table_name = f"{prefix}{index}"
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()

@measure_time
def create_db(dbname: str):
    """
    Tạo database mới nếu chưa tồn tại
    """
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    try:
        cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s', (dbname,))
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute(f'CREATE DATABASE {dbname}')
        else:
            print(f'Database {dbname} đã tồn tại')
    finally:
        cur.close()
        con.close()


