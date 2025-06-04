import time
import InterfaceDuckDB

DATABASE_NAME = 'dds_assgn1'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
INPUT_FILE_PATH = "C:/Users/MSI-PC/Documents/BTL-CSDLPT/ratings.dat"
N_PARTITIONS = 5

def main():
    conn = InterfaceDuckDB.getopenconnection(user='postgres', password='1234', dbname=DATABASE_NAME)

    # Load dữ liệu vào DB
    start = time.time()
    InterfaceDuckDB.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
    print(f"Load xong: {time.time() - start:.2f} giây")

    # Tạo range partition
    start = time.time()
    InterfaceDuckDB.rangepartition(RATINGS_TABLE, N_PARTITIONS, conn)
    print(f"Range partition xong: {time.time() - start:.2f} giây")

    # Tạo round robin partition
    start = time.time()
    InterfaceDuckDB.roundrobinpartition(RATINGS_TABLE, N_PARTITIONS, conn)
    print(f"Round robin partition xong: {time.time() - start:.2f} giây")

    # Chèn dòng mới
    InterfaceDuckDB.roundrobininsert(RATINGS_TABLE, 100, 200, 3.5, conn)
    InterfaceDuckDB.rangeinsert(RATINGS_TABLE, 101, 201, 4.0, conn)

    print("Đã thực hiện xong các thao tác với ratings.dat!")

    conn.close()

if __name__ == "__main__":
    main()
