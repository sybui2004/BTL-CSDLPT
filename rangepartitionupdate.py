RANGE_METADATA_TABLE = 'range_metadata'

def rangepartitionunloggedtable(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân mảnh thành nhiều bảng con dựa trên khoảng giá trị rating.
    Ví dụ: 0-1, >1-2, >2-3, ...
    """

    con = openconnection
    cur = con.cursor()

    delta = 5 / numberofpartitions          # Vì rating nằm trong khoảng [0, 5]
    RANGE_TABLE_PREFIX = "range_part"                   # Tiền tố tên bảng phân vùng

    # Tối ưu hiệu suất để insert nhanh
    cur.execute("SET synchronous_commit = OFF;")              # Không ghi log mỗi COMMIT (giảm I/O)
    cur.execute("SET work_mem = '1024MB';")                   # Tăng bộ nhớ cho các phép toán sort, hash
    cur.execute("SET maintenance_work_mem = '2097151kB';")    # Tăng bộ nhớ cho các thao tác CREATE TABLE, COPY

    for i in range(numberofpartitions):
        minR = i * delta
        maxR = minR + delta
        table = f"{RANGE_TABLE_PREFIX}{i}"

        # Tạo bảng con không ghi log (UNLOGGED) và chèn dữ liệu nằm trong khoảng minR - maxR
        # Với bảng đầu tiên (i == 0), bao gồm cả rating == minR (>= minR)
        # Các bảng sau thì loại bỏ minR (> minR) để tránh trùng lặp
        cur.execute(f"""
            CREATE UNLOGGED TABLE {table} AS
            SELECT userid, movieid, rating
            FROM {ratingstablename}
            WHERE rating {'>=' if i == 0 else '>'} {minR}
              AND rating <= {maxR};
        """)

    # Lưu thay đổi và đóng cursor
    cur.close()
    con.commit()

def rangepartitionbestchoice(ratingstablename, numberofpartitions, openconnection):
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