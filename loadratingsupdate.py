import psycopg2
from io import StringIO
import time
import functools
import duckdb
import os
import tempfile
import polars as pl

DATABASE_NAME = 'dds_assgn1'

def loadratingsusepolars(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection

    # Đọc file ratings sử dụng thư viện Polars
    # - Mỗi dòng được đọc là một chuỗi string duy nhất
    # - Dùng separator="\n" để mỗi dòng là một dòng riêng biệt
    # - Đặt tên cột là "line"
    lines = pl.read_csv(ratingsfilepath, separator="\n", has_header=False, new_columns=["line"])

    # Tách cột line thành danh sách các trường dựa vào ký tự "::"
    # - Sử dụng hàm str.split("::") để tách dòng
    # - Sau đó chọn từng phần tử: 0 là userid, 1 là movieid, 2 là rating
    # - Ép kiểu dữ liệu: Int32, Float32
    df = lines.with_columns([
        pl.col("line").str.split("::").alias("fields")
    ]).select([
        pl.col("fields").list.get(0).cast(pl.Int32).alias("userid"),
        pl.col("fields").list.get(1).cast(pl.Int32).alias("movieid"),
        pl.col("fields").list.get(2).cast(pl.Float32).alias("rating")
    ])

    # Ghi DataFrame ra một file CSV tạm để dùng trong COPY
    # - delete=False: không xóa file ngay lập tức để có thể đọc lại sau đó
    # - newline='': không thêm dòng trống thừa 
    with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix=".csv") as tmpfile:
        csv_path = tmpfile.name
        # Ghi dữ liệu dưới dạng CSV, không ghi header vì PostgreSQL COPY không cần
        df.write_csv(csv_path, has_header=False)

    # Tạo bảng trong PostgreSQL có 3 cột: userid, movieid, rating
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {ratingstablename} (userid INTEGER, movieid INTEGER, rating FLOAT);")

    # Import dữ liệu từ file CSV vào PostgreSQL bằng COPY
    with open(csv_path, 'r') as f:
        cur.copy_expert(f"COPY {ratingstablename} FROM STDIN WITH (FORMAT CSV)", f)

    # Lưu thay đổi và đóng cursor
    conn.commit()
    cur.close()

    # Xóa file tạm 
    os.remove(csv_path)
    
def loadratingsuseduckdb(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection  

    # Sử dụng DuckDB để đọc file gốc chứa dữ liệu ratings
    # - DuckDB đọc từng dòng trong file như 1 chuỗi
    # - Mỗi dòng có định dạng: "userid::movieid::rating::timestamp"
    # - split_part(line, '::', n) dùng để tách trường thứ n (bắt đầu từ 1)
    # - Ép kiểu dữ liệu thành INTEGER, FLOAT 
    # - Chỉ lấy những dòng hợp lệ
    con_duck = duckdb.connect()
    df = con_duck.execute(f"""
        SELECT 
            CAST(split_part(line, '::', 1) AS INTEGER) AS userid,
            CAST(split_part(line, '::', 2) AS INTEGER) AS movieid,
            CAST(split_part(line, '::', 3) AS FLOAT) AS rating
        FROM read_csv_auto('{ratingsfilepath}', delim='\n', header=False) AS t(line)
        WHERE line IS NOT NULL AND length(line) > 0
    """).fetchdf()
    con_duck.close()  # Đóng kết nối DuckDB sau khi đã lấy dữ liệu

    # Ghi dữ liệu từ DuckDB DataFrame ra một file CSV tạm
    # - tempfile.NamedTemporaryFile dùng để tạo file tạm, không xóa ngay (delete=False)
    # - newline='' để tránh lỗi ghi thừa dòng trên một số hệ điều hành
    # - suffix='.csv' đảm bảo đúng định dạng file CSV
    with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix=".csv") as tmpfile:
        csv_path = tmpfile.name  # Lưu lại đường dẫn của file tạm
        df.to_csv(csv_path, index=False, header=False)  # Ghi dữ liệu không có header, không có index

    # Tạo bảng trong PostgreSQL với 3 cột phù hợp: userid, movieid, rating
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {ratingstablename} (userid INTEGER, movieid INTEGER, rating FLOAT);")

    # Dùng lệnh COPY để nạp dữ liệu từ file CSV vào bảng PostgreSQL
    # - COPY FROM STDIN: nhập dữ liệu từ file thông qua Python file handle
    # - FORMAT CSV: định dạng dữ liệu là CSV (không có header, phân cách bằng dấu phẩy)
    with open(csv_path, 'r') as f:
        cur.copy_expert(f"COPY {ratingstablename} FROM STDIN WITH (FORMAT CSV)", f)

    # Lưu thay đổi và đóng cursor
    conn.commit()
    cur.close()

    # Xóa file tạm 
    os.remove(csv_path)
    
def loadratingsbestchoice(ratingstablename, ratingsfilepath, openconnection):
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
