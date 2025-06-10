import psycopg2
from io import StringIO
import duckdb
import os
import tempfile
import polars as pl

DATABASE_NAME = 'dds_assgn1'

def loadratingsnouselib(ratingstablename, ratingsfilepath, openconnection):
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

def loadratingsuseduckdb(ratingstablename, ratingsfilepath, openconnection):
    """
    Hàm load dữ liệu ratings từ file vào PostgreSQL sử dụng DuckDB làm công cụ ETL trung gian
    """
    # Tạo database
    create_db('dds_assgn1')
    
    # Sử dụng kết nối PostgreSQL được truyền vào
    conn = openconnection
    cur = conn.cursor()  # Tạo cursor để thực thi các lệnh SQL
    
    try:
        con_duck = duckdb.connect()
        
        # File ratings có format: userid::movieid::rating::timestamp
        # Định nghĩa 4 cột đầu vào đều là VARCHAR
        column_def = {
            'column1': 'VARCHAR',  # userid
            'column2': 'VARCHAR',  # movieid
            'column3': 'VARCHAR',  # rating 
            'column4': 'VARCHAR'   # timestamp 
        }
        
        # Tạo file CSV tạm để lưu dữ liệu đã được DuckDB xử lý và làm sạch
        # delete=False: giữ file sau khi đóng để PostgreSQL có thể đọc
        # newline='': tránh thêm dòng trống
        # suffix='.csv': file có đuôi .csv
        with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv') as tmpfile:
            csv_path = tmpfile.name  # Lưu đường dẫn file tạm
        
        # DuckDB thực hiện: đọc file -> transform -> xuất CSV
        con_duck.execute(f"""
            COPY (
                SELECT
                    CAST(column1 AS INTEGER) AS userid,      -- Chuyển đổi userid từ VARCHAR sang INTEGER
                    CAST(column2 AS INTEGER) AS movieid,     -- Chuyển đổi movieid từ VARCHAR sang INTEGER  
                    CAST(column3 AS FLOAT) AS rating         -- Chuyển đổi rating từ VARCHAR sang FLOAT
                FROM read_csv('{ratingsfilepath}',           -- Đọc file đầu vào
                    delim='::',                              -- Phân tách bằng dấu '::'
                    columns=$coldef,                         -- Sử dụng định nghĩa cột ở trên
                    header=False,                            -- File không có header
                    ignore_errors=True                       -- Bỏ qua các dòng lỗi
                )
            )
            TO '{csv_path}' (FORMAT CSV, HEADER FALSE);      -- Xuất ra file CSV tạm (không có header)
        """, {'coldef': column_def})
        
        # Đóng kết nối DuckDB 
        con_duck.close()
        
        # Tạo bảng ratings với 3 cột cần thiết (bỏ qua timestamp)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {ratingstablename} (
                userid INTEGER,    -- ID người dùng
                movieid INTEGER,   -- ID phim  
                rating FLOAT       -- Điểm đánh giá
            );
        """)
        
        # Mở file CSV tạm và copy trực tiếp vào PostgreSQL
        with open(csv_path, 'r') as f:
            # copy_expert: phương thức hiệu quả nhất để import bulk data vào PostgreSQL
            cur.copy_expert(f"COPY {ratingstablename} FROM STDIN WITH (FORMAT CSV)", f)
        
        # Lưu thay đổi
        conn.commit() 
        
    except Exception as e:
        # Nếu có lỗi xảy ra, rollback 
        conn.rollback()
        raise e 
        
    finally:
        # Đóng cursor
        cur.close() 
        
        # Xóa file tạm
        if os.path.exists(csv_path):
            os.remove(csv_path)

def loadratingusepolar(ratingstablename, ratingsfilepath, openconnection):
    # Tạo database 
    create_db('dds_assgn1')

    conn = openconnection
    cur = conn.cursor()

    try:
        # Tối ưu cấu hình PostgreSQL để tăng tốc độ INSERT/COPY
        cur.execute("SET synchronous_commit = OFF;")  # Không cần ghi log ngay mỗi lần COMMIT
        cur.execute("SET work_mem = '1024MB';")       # Tăng bộ nhớ RAM dùng cho các thao tác xử lý
        cur.execute("SET maintenance_work_mem = '2097151kB';")  # Tăng RAM cho CREATE TABLE và COPY

        # Tạo bảng nếu chưa có, với fillfactor=100 (giảm phân mảnh khi ghi nhiều)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {ratingstablename} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            ) WITH (fillfactor=100);
        """)

        # Đọc file ratings từ đường dẫn `ratingsfilepath`
        # - Mỗi dòng là một chuỗi string (dạng: "userid::movieid::rating::timestamp")
        # - Dùng Polars đọc theo dòng (mỗi dòng là 1 row duy nhất)
        lines = pl.read_csv(ratingsfilepath, separator="\n", has_header=False, new_columns=["line"])

        # Tách từng dòng thành danh sách các trường (userid, movieid, rating)
        df = lines.with_columns([
            pl.col("line").str.split("::").alias("fields")
        ]).select([
            pl.col("fields").list.get(0).cast(pl.Int32).alias("userid"),
            pl.col("fields").list.get(1).cast(pl.Int32).alias("movieid"),
            pl.col("fields").list.get(2).cast(pl.Float32).alias("rating")  # timestamp bỏ qua
        ])

        # Ghi dữ liệu Polars DataFrame ra một file CSV tạm thời (để COPY vào PostgreSQL)
        # - `include_header=False` vì PostgreSQL không cần dòng tiêu đề
        with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix=".csv") as tmpfile:
            csv_path = tmpfile.name
            df.write_csv(csv_path, include_header=False)

        # Sử dụng COPY để import dữ liệu nhanh từ file CSV vào bảng PostgreSQL
        with open(csv_path, 'r') as f:
            cur.copy_expert(f"COPY {ratingstablename} FROM STDIN WITH (FORMAT CSV)", f)

        # Lưu thay đổi
        conn.commit()

    except Exception as e:
        # Nếu có lỗi thì rollback
        conn.rollback()
        raise e

    finally:
        # Đóng cursor và xóa file tạm
        cur.close()
        if os.path.exists(csv_path):
            os.remove(csv_path)
    
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
