#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
from psycopg2.extras import execute_values

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection, chunksize=10000):
    """
    Load data from ratingsfilepath into ratingstablename in chunks.
    """

    con = openconnection
    cur = con.cursor()
    # Xóa bảng nếu đã tồn tại để tránh dữ liệu bị lặp
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
    # Tạo bảng mới
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        );
    """)
    con.commit()

    def parse_line(line):
        # Xử lý dòng dữ liệu: 1::122::5::838985046
        parts = line.strip().split("::")
        if len(parts) >= 3:
            return (int(parts[0]), int(parts[1]), float(parts[2]))
        return None

    batch = []
    with open(ratingsfilepath, "r") as f:
        for line in f:
            row = parse_line(line)
            if row:
                batch.append(row)
            if len(batch) >= chunksize:
                execute_values(
                    cur,
                    f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES %s",
                    batch
                )
                con.commit()
                batch = []
        # Insert phần còn lại
        if batch:
            execute_values(
                cur,
                f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES %s",
                batch
            )
            con.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    Each partition will contain records with ratings falling within specific ranges.
    For N partitions:
    - Range size = 5.0/N
    - Partition i (0 to N-1) contains:
        - i=0: ratings in [0, range_size]
        - i>0: ratings in (i*range_size, (i+1)*range_size]
    Also creates and maintains metadata table for partition information.
    """
    con = openconnection
    cur = con.cursor()
    
    # Create metadata table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RangePartitionsMetadata (
            partition_index INTEGER PRIMARY KEY,
            partition_table_name VARCHAR(255),
            min_rating_exclusive FLOAT,
            max_rating_inclusive FLOAT
        );
    """)
    
    # Clear existing metadata
    cur.execute("DELETE FROM RangePartitionsMetadata;")
    
    # Calculate range size and boundaries
    range_size = 5.0 / numberofpartitions
    boundaries = [i * range_size for i in range(numberofpartitions + 1)]
    
    # Create and populate each partition
    for i in range(numberofpartitions):
        table_name = f'range_part{i}'
        
        # Drop existing partition table if exists
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        # Create partition table with same schema as ratings table
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)
        
        # Calculate boundaries for this partition
        if i == 0:
            # First partition includes lower bound (0)
            min_rating_exclusive = -0.1  # Special value for first partition
            max_rating_inclusive = boundaries[1]
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating >= 0 AND rating <= {max_rating_inclusive};
            """)
        else:
            # Other partitions exclude lower bound
            min_rating_exclusive = boundaries[i]
            max_rating_inclusive = boundaries[i + 1]
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating > {min_rating_exclusive} AND rating <= {max_rating_inclusive};
            """)
        
        # Store partition metadata
        cur.execute("""
            INSERT INTO RangePartitionsMetadata 
            (partition_index, partition_table_name, min_rating_exclusive, max_rating_inclusive)
            VALUES (%s, %s, %s, %s);
        """, (i, table_name, min_rating_exclusive, max_rating_inclusive))
    
    con.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    Each partition will contain rows distributed in a round-robin fashion.
    Also creates and maintains metadata table for round-robin state.
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Create metadata table for round-robin state
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RoundRobinState (
                singleton_id INTEGER PRIMARY KEY,
                next_partition_index INTEGER,
                num_partitions INTEGER
            );
        """)
        
        # Clear existing metadata
        cur.execute("DELETE FROM RoundRobinState;")
        
        # Create partition tables
        for i in range(numberofpartitions):
            table_name = f'rrobin_part{i}'
            
            # Drop existing partition table if exists
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            
            # Create partition table with same schema as ratings table
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)
        
        # Distribute data in round-robin fashion using ROW_NUMBER()
        for i in range(numberofpartitions):
            cur.execute(f"""
                INSERT INTO rrobin_part{i} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM (
                    SELECT userid, movieid, rating,
                           ROW_NUMBER() OVER () as rnum
                    FROM {ratingstablename}
                ) as temp
                WHERE MOD(rnum - 1, {numberofpartitions}) = {i};
            """)
        
        # Initialize round-robin state
        cur.execute("""
            INSERT INTO RoundRobinState (singleton_id, next_partition_index, num_partitions)
            VALUES (1, 0, %s);
        """, (numberofpartitions,))
        
        con.commit()
        
    except Exception as e:
        con.rollback()
        print(f"Debug - Error in roundrobinpartition: {str(e)}")
        raise e
        
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach. Uses RoundRobinState metadata to determine the next partition.
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Start transaction
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        
        # 1. Insert into main ratings table
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(ratingstablename), (userid, itemid, rating))
        
        # 2. Get current round-robin state
        cur.execute("""
            SELECT next_partition_index, num_partitions 
            FROM RoundRobinState 
            WHERE singleton_id = 1;
        """)
        
        result = cur.fetchone()
        if result is None:
            # If RoundRobinState doesn't exist, create it
            cur.execute("""
                CREATE TABLE IF NOT EXISTS RoundRobinState (
                    singleton_id INTEGER PRIMARY KEY,
                    next_partition_index INTEGER,
                    num_partitions INTEGER
                );
            """)
            
            # Count existing partitions
            cur.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name LIKE 'rrobin_part%';
            """)
            num_partitions = cur.fetchone()[0]
            
            # Initialize state
            cur.execute("""
                INSERT INTO RoundRobinState (singleton_id, next_partition_index, num_partitions)
                VALUES (1, 0, %s);
            """, (num_partitions,))
            
            next_partition_index = 0
        else:
            next_partition_index, num_partitions = result
        
        # 3. Insert into the target partition
        target_table = f'rrobin_part{next_partition_index}'
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(target_table), (userid, itemid, rating))
        
        # 4. Calculate next partition index
        updated_next_index = (next_partition_index + 1) % num_partitions
        
        # 5. Update RoundRobinState
        cur.execute("""
            UPDATE RoundRobinState 
            SET next_partition_index = %s 
            WHERE singleton_id = 1;
        """, (updated_next_index,))
        
        # Commit transaction
        con.commit()
        print(f"Debug - Successfully inserted into {target_table}")
        
    except Exception as e:
        con.rollback()
        print(f"Debug - Error during insert: {str(e)}")
        raise e
        
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    Uses RangePartitionsMetadata to determine the correct partition.
    
    Args:
        ratingstablename: Name of the main ratings table
        userid: User ID of the new rating
        itemid: Movie ID of the new rating
        rating: Rating value
        openconnection: Database connection
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Start transaction
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        
        # 1. Insert into main ratings table
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(ratingstablename), (userid, itemid, rating))
        
        # 2. Find the correct partition using metadata
        cur.execute("""
            SELECT partition_table_name, min_rating_exclusive, max_rating_inclusive
            FROM RangePartitionsMetadata 
            WHERE %s > min_rating_exclusive 
            AND %s <= max_rating_inclusive;
        """, (rating, rating))
        
        result = cur.fetchone()
        if result is None:
            # If no partition found, try to find the partition that contains this rating
            cur.execute("""
                SELECT partition_table_name, min_rating_exclusive, max_rating_inclusive
                FROM RangePartitionsMetadata 
                ORDER BY min_rating_exclusive;
            """)
            all_partitions = cur.fetchall()
            print(f"Debug - Rating {rating} not found in any partition. Available partitions:")
            for p in all_partitions:
                print(f"Partition {p[0]}: {p[1]} < rating <= {p[2]}")
            raise Exception(f"No suitable partition found for rating value: {rating}")
            
        partition_table = result[0]
        print(f"Debug - Found partition {partition_table} for rating {rating}")
        
        # 3. Insert into the correct partition
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(partition_table), (userid, itemid, rating))
        
        # 4. Verify the insert
        cur.execute("""
            SELECT COUNT(*) FROM {} 
            WHERE userid = %s AND movieid = %s AND rating = %s;
        """.format(partition_table), (userid, itemid, rating))
        
        count = cur.fetchone()[0]
        if count != 1:
            raise Exception(f"Insert verification failed. Expected 1 row, found {count}")
        
        # Commit transaction
        con.commit()
        print(f"Debug - Successfully inserted into {partition_table}")
        
    except Exception as e:
        # Rollback in case of error
        con.rollback()
        print(f"Debug - Error during insert: {str(e)}")
        raise e
        
    finally:
        cur.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
