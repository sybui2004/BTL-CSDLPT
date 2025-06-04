import psycopg2
import psycopg2.extras
import os
import multiprocessing as mp
from gc import disable as gc_disable, enable as gc_enable

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(
    user: str = 'postgres', 
    password: str = '1234', 
    dbname: str = DATABASE_NAME
) -> psycopg2.extensions.connection:
    """Get an open connection to the database."""
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host='localhost',
            password=password
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise

def get_file_chunks(
    file_name: str, 
    max_cpu: int = 8
) -> tuple[int, list[tuple[str, int, int]]]:
    """Split file into chunks for parallel processing."""
    cpu_count = min(max_cpu, mp.cpu_count())
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    start_end = list()
    with open(file_name, mode="r") as f:
        def is_new_line(position):
            if position == 0:
                return True
            else:
                f.seek(position - 1)
                return f.read(1) == "\n"  # Sửa: bỏ 'b' vì đang mở file text mode

        def next_line(position):
            f.seek(position)
            f.readline()
            return f.tell()

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            while chunk_end < file_size and not is_new_line(chunk_end):
                chunk_end -= 1

            if chunk_start == chunk_end:
                chunk_end = next_line(chunk_end)

            start_end.append(
                (
                    file_name, 
                    chunk_start, 
                    chunk_end
                )
            )
            chunk_start = chunk_end

    return (cpu_count, start_end)

def process_ratings_chunk(
    file_name: str, 
    chunk_start: int, 
    chunk_end: int
) -> list[tuple[int, int, float]]:
    """Process a chunk of the ratings file using memory mapping."""
    results = []
    
    with open(file_name, mode="r") as f:
        f.seek(chunk_start)
            
        if chunk_start != 0:
            f.readline()
        pos = f.tell()
        while pos < chunk_end:
            line = f.readline()
            if not line:
                break
            pos = f.tell()
            parts = line.strip().split("::")
            if len(parts) >= 3:
                try:
                    userid = int(parts[0])
                    movieid = int(parts[1])
                    rating = float(parts[2])  # Giữ nguyên float
                    results.append((userid, movieid, rating))
                except Exception:
                    continue
    return results

def loadratings(ratingstablename: str, ratingsfilepath: str, openconnection):
    cpu_count, chunks = get_file_chunks(ratingsfilepath, max_cpu=mp.cpu_count())
    con = openconnection
    cur = con.cursor()
    try:
        # Drop and create table
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {ratingstablename} (
                userid INTEGER NOT NULL,
                movieid INTEGER NOT NULL,
                rating FLOAT NOT NULL CHECK (rating >= 0 AND rating <= 5.0)
            );
        """)

        # Process chunks in parallel
        with mp.Pool(cpu_count) as pool:
            chunk_results = pool.starmap(process_ratings_chunk, chunks)
        
        # Combine and insert results
        total_records = 0
        batch_size = 50000
        
        for chunk_data in chunk_results:
            if chunk_data:
                # Insert in batches
                for i in range(0, len(chunk_data), batch_size):
                    batch = chunk_data[i:i + batch_size]
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES %s",
                        batch,
                        template=None,
                        page_size=len(batch)
                    )
                    total_records += len(batch)
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        raise
    

def rangepartition(ratingstablename: str, numberofpartitions: int, openconnection):
    """Range partitioning for float ratings (0.0-5.0)."""
    if numberofpartitions <= 0:
        raise ValueError("Number of partitions must be positive")
    
    con = openconnection
    cur = con.cursor()
    try:
        delta = 5.0 / numberofpartitions  # Chia float 5.0 thay vì integer
        RANGE_TABLE_PREFIX = 'range_part'
            
        print(f"Creating {numberofpartitions} range partitions for float ratings")
        
        current_min = 0.0  # Bắt đầu từ 0.0
        for i in range(numberofpartitions):
            # Calculate range for this partition
            current_max = current_min + delta
            
            if i == numberofpartitions - 1:
                current_max = 5.0  # Đảm bảo partition cuối bao gồm 5.0
            
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER NOT NULL,
                    movieid INTEGER NOT NULL,
                    rating FLOAT NOT NULL
                );
            """)
            
            # Add indexes
            cur.execute(f"CREATE INDEX idx_{table_name}_userid ON {table_name}(userid);")
            cur.execute(f"CREATE INDEX idx_{table_name}_rating ON {table_name}(rating);")
            
            # Insert data
            if i == 0:
                cur.execute(f"""
                    INSERT INTO {table_name} 
                    SELECT userid, movieid, rating 
                    FROM {ratingstablename} 
                    WHERE rating >= %s AND rating <= %s;
                """, (current_min, current_max))
            else:
                cur.execute(f"""
                    INSERT INTO {table_name} 
                    SELECT userid, movieid, rating 
                    FROM {ratingstablename} 
                    WHERE rating > %s AND rating <= %s;
                """, (current_min, current_max))
            
            # Log partition info
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cur.fetchone()[0]
            print(f"Partition {table_name}: {count} records (rating: {current_min:.1f} - {current_max:.1f})")
            
            current_min = current_max
        
        openconnection.commit()
        print("Range partitioning completed")
            
    except Exception as e:
        openconnection.rollback()
        raise

def roundrobinpartition(ratingstablename: str, numberofpartitions: int, openconnection):
    """High-performance round robin partitioning using server-side processing."""
    if numberofpartitions <= 0:
        raise ValueError("Number of partitions must be positive")
    
    con = openconnection
    cur = con.cursor()
    try:
        RROBIN_TABLE_PREFIX = 'rrobin_part'        
        # Create partition tables
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER NOT NULL,
                    movieid INTEGER NOT NULL,
                    rating FLOAT NOT NULL
                );
            """)
            cur.execute(f"CREATE INDEX idx_{table_name}_userid ON {table_name}(userid);")
        
        # Use PostgreSQL's row_number() for efficient round robin distribution
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT userid, movieid, rating
                FROM (
                    SELECT userid, movieid, rating,
                            ROW_NUMBER() OVER() - 1 as rn
                    FROM {ratingstablename}
                ) t
                WHERE rn % %s = %s;
            """, (numberofpartitions, i))
            
            # Log partition info
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cur.fetchone()[0]
            print(f"Round robin partition {table_name}: {count} records")
        
        openconnection.commit()
        print("Round robin partitioning completed")
        
    except Exception as e:
        openconnection.rollback()
        raise

def roundrobininsert(ratingstablename: str, userid: int, itemid: int, rating: float, openconnection):
    """Insert với rating là float."""
    if rating < 0 or rating > 5:
        raise ValueError(f"Rating must be between 0 and 5, got {rating}")
    
    con = openconnection
    cur = con.cursor()
    try:
        RROBIN_TABLE_PREFIX = 'rrobin_part'
        
        # Insert into main table
        cur.execute(
            f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
            (userid, itemid, rating)  # Giữ nguyên float
        )
        
        # Get partition count and calculate index
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
        if numberofpartitions == 0:
            raise ValueError("No round robin partitions found")
        
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        total_rows = cur.fetchone()[0]
        index = (total_rows - 1) % numberofpartitions
        
        table_name = f"{RROBIN_TABLE_PREFIX}{index}"
        cur.execute(
            f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);",
            (userid, itemid, rating)  # Giữ nguyên float
        )
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        raise

def rangeinsert(ratingstablename: str, userid: int, itemid: int, rating: float, openconnection):
    """Insert với rating là float và tính toán range đúng."""
    if rating < 0 or rating > 5:
        raise ValueError(f"Rating must be between 0 and 5, got {rating}")
    
    con = openconnection
    cur = con.cursor()
    try:
        RANGE_TABLE_PREFIX = 'range_part'
        
        numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
        if numberofpartitions == 0:
            raise ValueError("No range partitions found")
        
        # Calculate partition index for float rating (0.0-5.0)
        delta = 5.0 / numberofpartitions
        
        # Find correct partition
        index = int(rating / delta)
        
        # Handle edge case when rating = 5.0
        if index >= numberofpartitions:
            index = numberofpartitions - 1
        
        table_name = f"{RANGE_TABLE_PREFIX}{index}"
        
        # Insert into both tables
        cur.execute(
            f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
            (userid, itemid, rating)  # Giữ nguyên float
        )
        cur.execute(
            f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);",
            (userid, itemid, rating)  # Giữ nguyên float
        )
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        raise

def create_db(dbname=DATABASE_NAME):
    """Create database with error handling."""
    try:
        con = getopenconnection(dbname='postgres')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(
            'SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s',
            (dbname,)
        )
        count = cur.fetchone()[0]
        
        if count == 0:
            cur.execute(f'CREATE DATABASE {dbname}')
            print(f"Database {dbname} created successfully")
        else:
            print(f"Database {dbname} already exists")
    except Exception as e:
        print(f"Error creating database: {e}")
        raise

def count_partitions(prefix: str, openconnection) -> int:
    """Count partitions with given prefix."""
    con = openconnection
    cur = con.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s AND table_type = 'BASE TABLE';",
        (f'{prefix}%',)
    )
    return cur.fetchone()[0]