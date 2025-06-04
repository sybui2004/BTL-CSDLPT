import Interface
import time
DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
INPUT_FILE_PATH = "C:/Users/MSI-PC/Documents/BTL-CSDLPT/ratings.dat"
N_PARTITIONS = 5

# Connect to database
conn = Interface.getopenconnection(user='postgres', password='1234', dbname=DATABASE_NAME)

start = time.time()

Interface.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
print("Load xong:", time.time() - start, "giây")

start = time.time()
Interface.rangepartition(RATINGS_TABLE, N_PARTITIONS, conn)
print("Range partition xong:", time.time() - start, "giây")

start = time.time()
Interface.roundrobinpartition(RATINGS_TABLE, N_PARTITIONS, conn)
print("Round robin partition xong:", time.time() - start, "giây")

# 4. Chèn một dòng mới theo round robin
Interface.roundrobininsert(RATINGS_TABLE, 100, 200, 3.5, conn)

# 5. Chèn một dòng mới theo range
Interface.rangeinsert(RATINGS_TABLE, 101, 201, 4.0, conn)

print("Đã thực hiện xong các thao tác với ratings.dat!")

# Đóng kết nối khi xong
conn.close() 

