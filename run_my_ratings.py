import Interface

DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = "C:/Users/MSI-PC/Documents/BTL-CSDLPT/ratings.dat"
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file

# Kết nối tới database (đổi user, password, dbname nếu cần)
conn = Interface.getopenconnection(user='postgres', password='1234', dbname=DATABASE_NAME)

# 1. Load dữ liệu vào bảng Ratings
Interface.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

# 2. Phân mảnh theo range
Interface.rangepartition(RATINGS_TABLE, 5, conn)

# 3. Phân mảnh theo round robin
Interface.roundrobinpartition(RATINGS_TABLE, 5, conn)

# 4. Chèn một dòng mới theo round robin
Interface.roundrobininsert(RATINGS_TABLE, 100, 200, 3.5, conn)

# 5. Chèn một dòng mới theo range
Interface.rangeinsert(RATINGS_TABLE, 101, 201, 4.0, conn)

print("Đã thực hiện xong các thao tác với ratings.dat!")

# Đóng kết nối khi xong
conn.close() 