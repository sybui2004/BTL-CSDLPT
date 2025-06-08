#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
from io import StringIO
import time
import functools
import duckdb
import os
import tempfile
import polars as pl

DATABASE_NAME = 'dds_assgn1'

RANGE_METADATA_TABLE = 'range_metadata'
RROBIN_METADATA_TABLE = 'rrobin_metadata'

# Hàm measure_time là một decorator dùng để đo thời gian thực thi của một hàm bất kỳ
def measure_time(func):
    # Dùng functools.wraps để giữ nguyên tên và docstring của hàm gốc khi được decor
    @functools.wraps(func)
    def wrapper_measure_time(*args, **kwargs):
        # Lấy thời gian bắt đầu trước khi gọi hàm gốc
        start = time.time()

        # Gọi hàm gốc với các đối số truyền vào
        result = func(*args, **kwargs)

        # Lấy thời gian sau khi hàm thực thi xong
        end = time.time()

        # In ra thời gian thực thi của hàm
        print(f"Hàm '{func.__name__}' thực thi trong {(end - start):.4f} giây.")

        # Trả về kết quả của hàm gốc
        return result

    # Trả về hàm đã decorated
    return wrapper_measure_time

# Hàm getopenconnection dùng để tạo và trả về một kết nối tới cơ sở dữ liệu PostgreSQL
def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    # Sử dụng hàm psycopg2.connect để tạo kết nối:
    # - dbname: tên cơ sở dữ liệu (mặc định là 'postgres')
    # - user: tên người dùng (mặc định là 'postgres')
    # - password: mật khẩu của người dùng (mặc định là '1234')
    # - host: máy chủ lưu trữ cơ sở dữ liệu (mặc định là 'localhost')
    return psycopg2.connect(
        "dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'"
    )


# Hàm khởi tạo bảng metadata cho kiểu phân vùng RANGE nếu chưa tồn tại.
def init_range_metadata_table(openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {RANGE_METADATA_TABLE} (
            tablename VARCHAR(100) PRIMARY KEY,        -- Tên bảng gốc
            partition_count INTEGER NOT NULL           -- Số lượng phân vùng RANGE cho bảng này
        );
    ''')
    con.commit()
    cur.close()

# Hàm khởi tạo bảng metadata cho kiểu phân vùng ROUND ROBIN nếu chưa tồn tại.
def init_rrobin_metadata_table(openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {RROBIN_METADATA_TABLE} (
            tablename VARCHAR(100) PRIMARY KEY,        -- Tên bảng gốc
            partition_count INTEGER NOT NULL,          -- Số lượng phân vùng ROUND ROBIN
            last_partition_index INTEGER NOT NULL      -- Chỉ số phân vùng cuối cùng đã được ghi vào (để biết phân vùng kế tiếp)
        );
    ''')
    con.commit()
    cur.close()

# Cập nhật metadata cho một bảng RANGE: xóa bản ghi cũ (nếu có), sau đó thêm mới.
def update_range_metadata(openconnection, tablename, partition_count):
    con = openconnection
    cur = con.cursor()
    cur.execute(f"DELETE FROM {RANGE_METADATA_TABLE} WHERE tablename = %s", (tablename,))
    cur.execute(
        f"INSERT INTO {RANGE_METADATA_TABLE} (tablename, partition_count) VALUES (%s, %s)",
        (tablename, partition_count)
    )
    con.commit()
    cur.close()

# Cập nhật metadata cho một bảng ROUND ROBIN: xóa bản ghi cũ (nếu có), sau đó thêm mới.
def update_rrobin_metadata(openconnection, tablename, partition_count, last_partition_index):
    con = openconnection
    cur = con.cursor()
    cur.execute(f"DELETE FROM {RROBIN_METADATA_TABLE} WHERE tablename = %s", (tablename,))
    cur.execute(
        f"INSERT INTO {RROBIN_METADATA_TABLE} (tablename, partition_count, last_partition_index) VALUES (%s, %s, %s)",
        (tablename, partition_count, last_partition_index)
    )
    con.commit()
    cur.close()


# Truy xuất số lượng phân vùng RANGE của bảng được chỉ định trong RANGE_METADATA.
# Trả về 0 nếu bảng không tồn tại trong RANGE_METADATA.
def get_range_metadata(openconnection, tablename):
    con = openconnection
    cur = con.cursor()
    cur.execute(
        f"SELECT partition_count FROM {RANGE_METADATA_TABLE} WHERE tablename = %s",
        (tablename,)
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0

# Truy xuất metadata của bảng ROUND ROBIN: gồm partition_count và chỉ số last_partition_index.
# Trả về (0, -1) nếu bảng không tồn tại trong metadata.
def get_rrobin_metadata(openconnection, tablename):
    con = openconnection
    cur = con.cursor()
    cur.execute(
        f"SELECT partition_count, last_partition_index FROM {RROBIN_METADATA_TABLE} WHERE tablename = %s",
        (tablename,)
    )
    row = cur.fetchone()
    cur.close()
    if row:
        return row[0], row[1]
    return 0, -1
    
@measure_time
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    # Tạo database 
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()

    try:
        # Tối ưu cấu hình PostgreSQL để tăng tốc độ ghi dữ liệu
        cur.execute("SET synchronous_commit = OFF;")  # Không ghi log mỗi COMMIT (giảm I/O)
        cur.execute("SET work_mem = '1024MB';")       # Tăng bộ nhớ cho các phép toán sort, hash
        cur.execute("SET maintenance_work_mem = '2097151kB';")  # Tăng bộ nhớ cho các thao tác CREATE TABLE, COPY

        # Tạo bảng ratings với fillfactor tối đa (giảm phân mảnh khi ghi dữ liệu liên tục)
        cur.execute(f"""
            CREATE TABLE {ratingstablename}(
                userid INTEGER, 
                movieid INTEGER, 
                rating FLOAT
            ) WITH (fillfactor=100);
        """)

        # Xử lý file input trước khi nạp: dùng bộ đệm lớn để giảm số lần đọc đĩa
        processed_data = StringIO()

        with open(ratingsfilepath, 'r', encoding='utf-8', buffering=65536) as f:
            for line in f:
                parts = line.strip().split("::")  # Tách dòng theo '::'
                if len(parts) >= 4:
                    # Ghi ra format chuẩn: userid \t movieid \t rating
                    processed_data.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")

        processed_data.seek(0)  # Quay về đầu file tạm

        # COPY trực tiếp từ buffer vào PostgreSQL
        cur.copy_from(processed_data, ratingstablename, sep='\t', size=65536)

        # Lưu thay đổi và đóng cursor
        cur.close()
        con.commit()

    except Exception as e:
        # Nếu có lỗi thì rollback và đóng cursor
        con.rollback()
        cur.close()
        raise e

@measure_time
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Phân mảnh bảng ratings thành nhiều bảng con dựa trên khoảng giá trị rating.
    Ví dụ: 0-1, >1-2, >2-3, ...
    """

    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'   # Tiền tố cho tên bảng con

    try:
        # Khởi tạo bảng range_metadata
        init_range_metadata_table(openconnection)
        # Tối ưu hiệu suất INSERT/CREATE TABLE:
        cur.execute("SET synchronous_commit = OFF;")           # Tắt ghi log mỗi COMMIT để giảm I/O
        cur.execute("SET work_mem = '1024MB';")                # Tăng RAM cho các thao tác tính toán trung gian
        cur.execute("SET maintenance_work_mem = '2097151kB';") # Tăng RAM cho CREATE TABLE, COPY, ...

        # Tính độ rộng mỗi khoảng rating
        delta = 5.0 / numberofpartitions

        # Tạo từng bảng con theo khoảng
        for i in range(numberofpartitions):
            minR = i * delta             # Cận dưới của khoảng
            maxR = minR + delta          # Cận trên của khoảng
            table = f"{RANGE_TABLE_PREFIX}{i}"       # Tên bảng: range_part0, range_part1, ...

            # Tạo bảng mới với dữ liệu trong khoảng [minR, maxR]
            # Với i == 0 thì dùng >= minR, còn lại thì dùng > minR để tránh trùng
            cur.execute(f"""
                CREATE TABLE {table} AS
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating {'>=' if i == 0 else '>'} {minR}
                  AND rating <= {maxR};
            """)
        # Cập nhật bảng range_metadata
        update_range_metadata(openconnection, ratingstablename, numberofpartitions)
        # Lưu tất cả thay đổi
        con.commit()

    except Exception as e:
        # Nếu có lỗi thì rollback 
        con.rollback()
        raise e

    finally:
        # Đóng cursor 
        cur.close()
        
@measure_time
def roundrobinpartition(ratingstablename: str, numberofpartitions: int, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    temp_table = "rrobin_temp"
    
    try:
        # Khởi tạo bảng metadata theo dõi trạng thái round-robin
        init_rrobin_metadata_table(con)
        
        # Thiết lập PostgreSQL để tối ưu hiệu suất
        cur.execute("SET synchronous_commit = OFF;")            # Tắt ghi log để tăng tốc độ insert
        cur.execute("SET work_mem = '1024MB';")                 # Tăng bộ nhớ xử lý sort/hash
        cur.execute("SET maintenance_work_mem = '2097151kB';")  # Tăng bộ nhớ cho thao tác CREATE TABLE

        # Tạo bảng tạm chứa dữ liệu và chỉ số phân vùng được tính trước
        cur.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT userid, movieid, rating,
                   (ROW_NUMBER() OVER () - 1) AS rnum,
                   (ROW_NUMBER() OVER () - 1) % {numberofpartitions} AS partition_id
            FROM {ratingstablename};
        """)
        
        # Tạo từng bảng con tương ứng với mỗi partition và chèn dữ liệu phù hợp
        for i in range(numberofpartitions):
            part_table = f"{RROBIN_TABLE_PREFIX}{i}"
            
            # Tạo bảng mới chứa dữ liệu thuộc partition i
            cur.execute(f"""
                CREATE TABLE {part_table} AS
                SELECT userid, movieid, rating 
                FROM {temp_table} 
                WHERE partition_id = {i};
            """)
        
        # Xóa bảng tạm sau khi hoàn tất chia partition
        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")

        # Tính tổng số dòng ban đầu để xác định partition cuối cùng được sử dụng
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cur.fetchone()[0]
        
        # Nếu có dữ liệu, xác định partition cuối (để tiếp tục round-robin sau này)
        last_partition_index = (total_rows - 1) % numberofpartitions if total_rows > 0 else -1

        # Cập nhật metadata về round-robin partition
        update_rrobin_metadata(con, ratingstablename, numberofpartitions, last_partition_index)

        # Lưu thay đổi
        con.commit()

    except Exception as e:
        # Nếu có lỗi, rollback 
        con.rollback()
        raise e
    finally:
        # Đóng cursor
        cur.close()


@measure_time
def roundrobininsert(ratingstablename: str, userid: int, itemid: int, rating: float, openconnection):
    """
    Hàm để thêm 1 dòng mới vào trong bảng chính và phân mảnh dựa trên phương pháp round robin
    """
    # Dùng kết nối cơ sở dữ liệu được truyền vào
    con = openconnection
    cur = con.cursor()

    # Tiền tố tên bảng cho các phân mảnh round robin
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    try:
        # Lấy tổng số phân mảnh round robin và chỉ số của phân mảnh cuối cùng vừa được thêm dữ liệu từ bảng metadata
        partition_count, last_partition_index = get_rrobin_metadata(con, ratingstablename)
        
        # Tính toán chỉ số của phân mảnh round robin thích hợp
        partition_index = (last_partition_index + 1) % partition_count

        # Tên bảng phân mảnh round robin tương ứng
        table_name = f"{RROBIN_TABLE_PREFIX}{partition_index}"

        # Thêm dòng mới vào phân mảnh round robin tương ứng
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))

        # Cập nhật metadata cho phân mảnh round robin
        update_rrobin_metadata(con, ratingstablename, partition_count, partition_index)

        # Commit các thay đổi
        con.commit()
    except Exception as e:
        # Rollback các thay đổi nếu có lỗi
        con.rollback()
        raise e
    finally:
        # Đóng cursor
        cur.close()


@measure_time
def rangeinsert(ratingstablename: str, userid: int, itemid: int, rating: float, openconnection):
    """
    Hàm để thêm 1 dòng mới vào trong bảng chính và phân mảnh dựa trên phương pháp range
    """
    # Dùng kết nối cơ sở dữ liệu được truyền vào
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    try:
        # Lấy tổng số phân mảnh range từ bảng metadata
        partition_count = get_range_metadata(con, ratingstablename)
        
        # Tính toán chỉ số của phân mảnh range thích hợp
        delta = 5.0 / partition_count
        index = int(rating / delta)
        if rating % delta == 0 and index != 0:
            index = index - 1

        # Tên bảng phân mảnh range tương ứng
        table_name = f"{RANGE_TABLE_PREFIX}{index}"

        # Thêm dòng mới vào phân mảnh range tương ứng
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))

        # Commit các thay đổi
        con.commit()
    except Exception as e:
        # Rollback các thay đổi nếu có lỗi
        con.rollback()
        raise e
    finally:
        # Đóng cursor
        cur.close()

@measure_time
def create_db(dbname):
    """
    Tạo một cơ sở dữ liệu mới trong PostgreSQL.
    Nếu cơ sở dữ liệu có tên `dbname` đã tồn tại thì không tạo mới.
    """

    # Kết nối tới cơ sở dữ liệu mặc định là 'postgres'
    con = getopenconnection(dbname='postgres')

    # Thiết lập chế độ AUTOCOMMIT vì lệnh CREATE DATABASE không thể nằm trong transaction
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Tạo cursor để thực hiện truy vấn SQL
    cur = con.cursor()

    # Kiểm tra xem cơ sở dữ liệu tên dbname đã tồn tại chưa
    cur.execute(
        'SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,)
    )
    count = cur.fetchone()[0]

    if count == 0:
        # Nếu chưa tồn tại thì tạo mới cơ sở dữ liệu
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        # Nếu đã tồn tại thì in thông báo
        print('A database named {0} already exists'.format(dbname))

    # Đóng cursor và kết nối
    cur.close()



