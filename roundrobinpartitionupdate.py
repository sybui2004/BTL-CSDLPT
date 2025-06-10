RROBIN_METADATA_TABLE = 'rrobin_metadata'

def roundrobinpartitionunloggedtable(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân mảnh bảng rating theo phương pháp Round Robin.
    Dữ liệu được chia đều lần lượt vào các bảng con theo thứ tự dòng.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'  # Tiền tố tên bảng con
    temp_table = "rrobin_temp"           # Bảng tạm để đánh số dòng

    # Tối ưu hiệu suất để insert nhanh
    cur.execute("SET synchronous_commit = OFF;")              # Không ghi log mỗi COMMIT (giảm I/O)
    cur.execute("SET work_mem = '1024MB';")                   # Tăng bộ nhớ cho các phép toán sort, hash
    cur.execute("SET maintenance_work_mem = '2097151kB';")    # Tăng bộ nhớ cho các thao tác CREATE TABLE, COPY

    # Tạo bảng tạm chứa dữ liệu gốc, thêm số thứ tự dòng và chỉ số phân vùng
    cur.execute(f"""
        CREATE UNLOGGED TABLE {temp_table} AS
        SELECT userid, movieid, rating,
               (ROW_NUMBER() OVER () - 1) AS rnum,                     -- Đánh số dòng từ 0
               (ROW_NUMBER() OVER () - 1) % {numberofpartitions} AS partition_id  -- Xác định phân vùng
        FROM {ratingstablename};
    """)

    # Tạo các bảng phân vùng và chèn dữ liệu tương ứng
    for i in range(numberofpartitions):
        part_table = f"{RROBIN_TABLE_PREFIX}{i}"  # Tên bảng phân vùng

        # Tạo bảng con và chèn các dòng có partition_id = i từ bảng tạm
        cur.execute(f"""
            CREATE UNLOGGED TABLE {part_table} AS
            SELECT userid, movieid, rating
            FROM {temp_table}
            WHERE partition_id = {i};
        """)

    # Xóa bảng tạm để giải phóng tài nguyên
    cur.execute(f"DROP TABLE IF EXISTS {temp_table};")

    # Lưu thay đổi và đóng cursor
    con.commit()
    cur.close()

def roundrobinpartitionbestchoice(ratingstablename: str, numberofpartitions: int, openconnection):
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
