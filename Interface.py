#!/usr/bin/python2.7
#
# Interface cho bài tập phân tán
#

import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env
load_dotenv()

# Lấy cấu hình database từ biến môi trường
DATABASE_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


def getopenconnection(user=None, password=None, dbname=None):
    """
    Hàm tạo kết nối đến database sử dụng biến môi trường hoặc tham số được truyền vào
    
    Args:
        user: Tên người dùng database (tùy chọn)
        password: Mật khẩu database (tùy chọn)
        dbname: Tên database (tùy chọn)
    
    Returns:
        Kết nối đến database
    """
    # Sử dụng biến môi trường nếu không có tham số được truyền vào
    user = user or DB_USER
    password = password or DB_PASSWORD
    dbname = dbname or DATABASE_NAME
    
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=DB_HOST,
        port=DB_PORT
    )


def loadratings(ratingstablename, ratingsfilepath, openconnection, chunksize=10000):
    """
    Hàm tải dữ liệu từ file ratings vào bảng ratings trong database.
    Dữ liệu được tải theo từng chunk để tối ưu hiệu suất.

    Args:
        ratingstablename: Tên bảng ratings
        ratingsfilepath: Đường dẫn đến file chứa dữ liệu ratings
        openconnection: Kết nối database
        chunksize: Kích thước mỗi chunk khi tải dữ liệu (mặc định: 10000)
    """
    con = openconnection
    cur = con.cursor()
    # Xóa bảng nếu đã tồn tại để tránh dữ liệu bị lặp
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
    # Tạo bảng mới với schema phù hợp
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        );
    """)
    con.commit()

    def parse_line(line):
        # Xử lý dòng dữ liệu theo định dạng: userid::movieid::rating::timestamp
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
        # Chèn phần dữ liệu còn lại
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
    Hàm tạo các partition của bảng chính dựa trên khoảng giá trị của ratings.
    Mỗi partition sẽ chứa các bản ghi có rating nằm trong một khoảng cụ thể.
    
    Với N partition:
    - Kích thước khoảng = 5.0/N
    - Partition i (0 đến N-1) chứa:
        - i=0: ratings trong [0, range_size]
        - i>0: ratings trong (i*range_size, (i+1)*range_size]
    
    Đồng thời tạo và duy trì bảng metadata để lưu thông tin về các partition.

    Args:
        ratingstablename: Tên bảng ratings
        numberofpartitions: Số lượng partition cần tạo
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    
    # Tạo bảng metadata nếu chưa tồn tại
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RangePartitionsMetadata (
            partition_index INTEGER PRIMARY KEY,
            partition_table_name VARCHAR(255),
            min_rating_exclusive FLOAT,
            max_rating_inclusive FLOAT
        );
    """)
    
    # Xóa metadata cũ
    cur.execute("DELETE FROM RangePartitionsMetadata;")
    
    # Tính toán kích thước khoảng và ranh giới
    range_size = 5.0 / numberofpartitions
    boundaries = [i * range_size for i in range(numberofpartitions + 1)]
    
    # Tạo và điền dữ liệu cho từng partition
    for i in range(numberofpartitions):
        table_name = f'range_part{i}'
        
        # Xóa bảng partition nếu đã tồn tại
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        # Tạo bảng partition với schema giống bảng ratings
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)
        
        # Tính toán ranh giới cho partition này
        if i == 0:
            # Partition đầu tiên bao gồm cả giá trị 0
            min_rating_exclusive = -0.1  # Giá trị đặc biệt cho partition đầu tiên
            max_rating_inclusive = boundaries[1]
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating >= 0 AND rating <= {max_rating_inclusive};
            """)
        else:
            # Các partition khác loại trừ giá trị dưới
            min_rating_exclusive = boundaries[i]
            max_rating_inclusive = boundaries[i + 1]
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating > {min_rating_exclusive} AND rating <= {max_rating_inclusive};
            """)
        
        # Lưu metadata của partition
        cur.execute("""
            INSERT INTO RangePartitionsMetadata 
            (partition_index, partition_table_name, min_rating_exclusive, max_rating_inclusive)
            VALUES (%s, %s, %s, %s);
        """, (i, table_name, min_rating_exclusive, max_rating_inclusive))
    
    con.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm tạo các partition của bảng chính sử dụng phương pháp round robin.
    Mỗi partition sẽ chứa các dòng được phân phối theo kiểu round-robin.
    Đồng thời tạo và duy trì bảng metadata để lưu trạng thái round-robin.

    Args:
        ratingstablename: Tên bảng ratings
        numberofpartitions: Số lượng partition cần tạo
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Tạo bảng metadata cho trạng thái round-robin
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RoundRobinState (
                singleton_id INTEGER PRIMARY KEY,
                next_partition_index INTEGER,
                num_partitions INTEGER
            );
        """)
        
        # Xóa metadata cũ
        cur.execute("DELETE FROM RoundRobinState;")
        
        # Tạo các bảng partition
        for i in range(numberofpartitions):
            table_name = f'rrobin_part{i}'
            
            # Xóa bảng partition nếu đã tồn tại
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            
            # Tạo bảng partition với schema giống bảng ratings
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)
        
        # Phân phối dữ liệu theo kiểu round-robin sử dụng ROW_NUMBER()
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
        
        # Khởi tạo trạng thái round-robin
        cur.execute("""
            INSERT INTO RoundRobinState (singleton_id, next_partition_index, num_partitions)
            VALUES (1, 0, %s);
        """, (numberofpartitions,))
        
        con.commit()
        
    except Exception as e:
        con.rollback()
        print(f"Debug - Lỗi trong roundrobinpartition: {str(e)}")
        raise e
        
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn một dòng mới vào bảng chính và partition cụ thể dựa trên phương pháp round robin.
    Sử dụng bảng metadata RoundRobinState để xác định partition tiếp theo.

    Args:
        ratingstablename: Tên bảng ratings
        userid: ID người dùng
        itemid: ID phim
        rating: Giá trị rating
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Bắt đầu transaction
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        
        # 1. Chèn vào bảng ratings chính
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(ratingstablename), (userid, itemid, rating))
        
        # 2. Lấy trạng thái round-robin hiện tại
        cur.execute("""
            SELECT next_partition_index, num_partitions 
            FROM RoundRobinState 
            WHERE singleton_id = 1;
        """)
        
        result = cur.fetchone()
        if result is None:
            # Nếu RoundRobinState chưa tồn tại, tạo mới
            cur.execute("""
                CREATE TABLE IF NOT EXISTS RoundRobinState (
                    singleton_id INTEGER PRIMARY KEY,
                    next_partition_index INTEGER,
                    num_partitions INTEGER
                );
            """)
            
            # Đếm số partition hiện có
            cur.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name LIKE 'rrobin_part%';
            """)
            num_partitions = cur.fetchone()[0]
            
            # Khởi tạo trạng thái
            cur.execute("""
                INSERT INTO RoundRobinState (singleton_id, next_partition_index, num_partitions)
                VALUES (1, 0, %s);
            """, (num_partitions,))
            
            next_partition_index = 0
        else:
            next_partition_index, num_partitions = result
        
        # 3. Chèn vào partition đích
        target_table = f'rrobin_part{next_partition_index}'
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(target_table), (userid, itemid, rating))
        
        # 4. Tính toán index partition tiếp theo
        updated_next_index = (next_partition_index + 1) % num_partitions
        
        # 5. Cập nhật RoundRobinState
        cur.execute("""
            UPDATE RoundRobinState 
            SET next_partition_index = %s 
            WHERE singleton_id = 1;
        """, (updated_next_index,))
        
        con.commit()
        print(f"Debug - Đã chèn thành công vào {target_table}")
        
    except Exception as e:
        con.rollback()
        print(f"Debug - Lỗi trong quá trình chèn: {str(e)}")
        raise e
        
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn một dòng mới vào bảng chính và partition cụ thể dựa trên giá trị rating.
    Sử dụng bảng metadata RangePartitionsMetadata để xác định partition phù hợp.

    Args:
        ratingstablename: Tên bảng ratings
        userid: ID người dùng
        itemid: ID phim
        rating: Giá trị rating
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Bắt đầu transaction
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        
        # 1. Chèn vào bảng ratings chính
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(ratingstablename), (userid, itemid, rating))
        
        # 2. Tìm partition phù hợp sử dụng metadata
        cur.execute("""
            SELECT partition_table_name, min_rating_exclusive, max_rating_inclusive
            FROM RangePartitionsMetadata 
            WHERE %s > min_rating_exclusive 
            AND %s <= max_rating_inclusive;
        """, (rating, rating))
        
        result = cur.fetchone()
        if result is None:
            # Nếu không tìm thấy partition, thử tìm partition chứa rating này
            cur.execute("""
                SELECT partition_table_name, min_rating_exclusive, max_rating_inclusive
                FROM RangePartitionsMetadata 
                ORDER BY min_rating_exclusive;
            """)
            all_partitions = cur.fetchall()
            print(f"Debug - Không tìm thấy partition cho rating {rating}. Các partition có sẵn:")
            for p in all_partitions:
                print(f"Partition {p[0]}: {p[1]} < rating <= {p[2]}")
            raise Exception(f"Không tìm thấy partition phù hợp cho giá trị rating: {rating}")
            
        partition_table = result[0]
        print(f"Debug - Đã tìm thấy partition {partition_table} cho rating {rating}")
        
        # 3. Chèn vào partition phù hợp
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """.format(partition_table), (userid, itemid, rating))
        
        # 4. Kiểm tra việc chèn
        cur.execute("""
            SELECT COUNT(*) FROM {} 
            WHERE userid = %s AND movieid = %s AND rating = %s;
        """.format(partition_table), (userid, itemid, rating))
        
        count = cur.fetchone()[0]
        if count != 1:
            raise Exception(f"Kiểm tra chèn thất bại. Mong đợi 1 dòng, tìm thấy {count}")
        
        con.commit()
        print(f"Debug - Đã chèn thành công vào {partition_table}")
        
    except Exception as e:
        # Rollback nếu có lỗi
        con.rollback()
        print(f"Debug - Lỗi trong quá trình chèn: {str(e)}")
        raise e
        
    finally:
        cur.close()

def create_db(dbname):
    """
    Tạo database bằng cách kết nối đến user và database mặc định của Postgres.
    Hàm kiểm tra xem database đã tồn tại chưa, nếu chưa thì tạo mới.

    Args:
        dbname: Tên database cần tạo
    """
    # Kết nối đến database mặc định
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Kiểm tra xem database đã tồn tại chưa
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Tạo database
    else:
        print('Database "{0}" đã tồn tại'.format(dbname))

    # Dọn dẹp
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Hàm đếm số lượng bảng có chứa @prefix trong tên.

    Args:
        prefix: Tiền tố cần tìm
        openconnection: Kết nối database

    Returns:
        Số lượng bảng tìm thấy
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
