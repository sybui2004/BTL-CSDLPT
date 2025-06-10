
import psycopg2
DATABASE_NAME = 'dds_assgn1'
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
    
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Đọc dữ liệu từ file (ratingsfilepath) và chen vào bang ratings trong cơ sở dữ liệu.
    """

    # Gọi hàm tạo database
    create_db(DATABASE_NAME)

    # Dùng kết nõi cơ sở dữ liệu được truyền vào
    con = openconnection
    cur = con.cursor()

    # Tạo bảng mới với tên là ratingstablename
    # Vì dữ liệu trong file có dấu ':' giữa các trường, cần tạo thêm các cột "extra" để chữa dau ":' không cần thiết
    cur.execute(
    "create table " + ratingstablename + " (" 
    "userid integer, "                        # userid
    "extra1 char, "                           # Dấu ':' đầu tiền giữa userid và movieid
    "movieid integer, "                       # movieid
    "extra2 char, "                           # Dấu ':' thứ hai giữa movieid và rating
    "rating float, "                          # rating
    "extra3 char, "                           # Dấu ':' thứ ba giữa rating và timestamp
    "timestamp bigint);"                      # timestamp
    )
    # Đọc dữ liệu từ file (phân tách bằng dấu ":") và nạp vào bằng
    cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')

    # Xóa các cột 'extra' va "timestamp'
    cur.execute(
    "alter table " + ratingstablename +
    " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;"
    )
    # Đóng cursor và lưu thay đổi vào cơ sở dữ liệu
    cur.close()
    con.commit()
    
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân mảnh thành nhiều bảng con dựa trên khoảng giá trị rating.
    """

    con = openconnection
    cur = con.cursor()

    # Tính khoảng giá trị rating cho mỗi phân vùng (delta)
    delta = 5 / numberofpartitions  # Vì rating nằm trong khoảng [0, 5]

    RANGE_TABLE_PREFIX = 'range_part'  # Tiền tố tên bảng con

    # Lặp qua từng phân vùng
    for i in range(0, numberofpartitions):
        minRange = i * delta         # Cận dưới của phân vùng
        maxRange = minRange + delta  # Cận trên của phân vùng

        table_name = RANGE_TABLE_PREFIX + str(i)  # Tên bảng phân vùng

        # Tạo bảng mới với cấu trúc giống bảng gốc
        cur.execute("CREATE TABLE " + table_name + " (userid INTEGER, movieid INTEGER, rating FLOAT);")

        # Chèn dữ liệu từ bảng gốc vào bảng phân vùng tương ứng
        if i == 0:
            # Phân vùng đầu tiên lấy cả min và max (>= min và <= max)
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating >= {minRange} AND rating <= {maxRange};
            """)
        else:
            # Các phân vùng sau chỉ lấy rating > min và <= max
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating > {minRange} AND rating <= {maxRange};
            """)

    # Lưu thay đổi và đóng cursor
    cur.close()
    con.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân mảnh bảng dữ liệu theo phương pháp round-robin.
    Mỗi dòng được phân phối tuần tự vào các bảng con.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'  # Tiền tố tên bảng con

    for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)

        # Tạo bảng con mới với cấu trúc (userid, movieid, rating)
        cur.execute("CREATE TABLE " + table_name + " (userid INTEGER, movieid INTEGER, rating FLOAT);")

        # Chèn dữ liệu vào bảng con theo thứ tự vòng tròn:
        # Sử dụng ROW_NUMBER để đánh số dòng, rồi chọn những dòng sao cho:
        # (row_number - 1) % numberofpartitions == i
        # phân phối tuần tự vào từng bảng
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM (
                SELECT userid, movieid, rating,
                    ROW_NUMBER() OVER () AS rnum
                FROM {ratingstablename}
            ) AS temp
            WHERE MOD(temp.rnum - 1, {numberofpartitions}) = {i};
        """)

    # Lưu thay đổi và đóng cursor
    cur.close()
    con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm để thêm 1 dòng mới vào trong bảng chính và phân mảnh dựa trên phương pháp round robin
    """
    # Dùng kết nối cơ sở dữ liệu được truyền vào
    con = openconnection
    cur = con.cursor()

    # Tiền tố tên bảng cho các phân mảnh round robin
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Thêm dòng mới vào bảng chính
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")

    # Lấy tổng số dòng trong bảng chính
    cur.execute("select count(*) from " + ratingstablename + ";");
    total_rows = (cur.fetchall())[0][0]

    # Lấy tổng số phân mảnh round robin
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)

    # Tính toán chỉ số của phân mảnh round robin
    index = (total_rows-1) % numberofpartitions

    # Tên bảng phân mảnh round robin
    table_name = RROBIN_TABLE_PREFIX + str(index)

    # Thêm dòng mới vào phân mảnh round robin tương ứng
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")

    # Đóng cursor và commit
    cur.close()
    con.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm để thêm 1 dòng mới vào trong bảng chính và phân mảnh dựa trên phương pháp range
    """
    # Dùng kết nối cơ sở dữ liệu được truyền vào
    con = openconnection
    cur = con.cursor()

    # Tiền tố tên bảng cho các phân mảnh range
    RANGE_TABLE_PREFIX = 'range_part'

    # Lấy tổng số phân mảnh range
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)

    # Tính toán chỉ số của phân mảnh range thích hợp
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1

    # Tên bảng phân mảnh range tương ứng
    table_name = RANGE_TABLE_PREFIX + str(index)

    # Thêm dòng mới vào phân mảnh range tương ứng
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")

    # Đóng cursor và commit
    cur.close()
    con.commit()

    
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


def count_partitions(prefix, openconnection):
    """
    Đếm số lượng bảng trong cơ sở dữ liệu hiện tại mà tên bảng bắt đầu bằng chuỗi `prefix`.
    """

    con = openconnection  # Lấy kết nối tới cơ sở dữ liệu
    cur = con.cursor()    # Tạo cursor để thao tác với CSDL

    # Truy vấn đếm số bảng do người dùng tạo mà có tên bắt đầu bằng `prefix`
    cur.execute(
        "SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '" + prefix + "%';"
    )

    count = cur.fetchone()[0]  # Lấy kết quả đếm được

    cur.close()  # Đóng cursor

    return count  # Trả về số lượng bảng phù hợp