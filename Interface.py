#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='admin123', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Tối ưu load file ratings.dat với format UserID::ItemID::Rating::Timestamp
    Xử lý 10M54 dòng hiệu quả
    """
    cur = openconnection.cursor()
    
    # Tạo table và tối ưu settings
    cur.execute(f"""
        DROP TABLE IF EXISTS {ratingstablename};
        CREATE TABLE {ratingstablename} (
            userid INTEGER,
            movieid INTEGER, 
            rating FLOAT
        );
        SET synchronous_commit = OFF;
    """)
    
    # Xử lý file theo batch để tránh memory overflow
    batch_size = 50000
    batch_data = []
    
    with open(ratingsfilepath, 'r') as file:
        for line_num, line in enumerate(file, 1):
            parts = line.strip().split('::')  # Format UserID::ItemID::Rating::Timestamp
            
            # Lấy userid, itemid, rating (bỏ qua timestamp)
            if len(parts) >= 4:
                try:
                    userid = parts[0]
                    movieid = parts[1]  # ItemID 
                    rating = parts[2]   # Rating
                    batch_data.append(f"{userid},{movieid},{rating}")
                except (ValueError, IndexError):
                    continue
            
            # Load batch khi đủ size
            if len(batch_data) >= batch_size:
                csv_data = '\n'.join(batch_data)
                from io import StringIO
                buffer = StringIO(csv_data)
                cur.copy_expert(f"COPY {ratingstablename} FROM STDIN WITH (FORMAT CSV)", buffer)
                batch_data = []
                if line_num % 500000 == 0:  # Progress every 500k rows
                    print(f"Processed {line_num:,} rows...")
    
    # Load batch cuối cùng
    if batch_data:
        csv_data = '\n'.join(batch_data)
        from io import StringIO
        buffer = StringIO(csv_data)
        cur.copy_expert(f"COPY {ratingstablename} FROM STDIN WITH (FORMAT CSV)", buffer)
    
    # Tạo index để tăng tốc query
    cur.execute(f"""
        CREATE INDEX idx_{ratingstablename}_userid ON {ratingstablename}(userid);
        CREATE INDEX idx_{ratingstablename}_movieid ON {ratingstablename}(movieid);
        ANALYZE {ratingstablename};
    """)
    
    openconnection.commit()
    cur.close()


# def loadratings(ratingstablename, ratingsfilepath, openconnection): 
#     """
#     Function to load data in @ratingsfilepath file to a table called @ratingstablename.
#     """
#     con = openconnection
#     cur = con.cursor()

#     # Drop table if exists and create new one
#     cur.execute("DROP TABLE IF EXISTS " + ratingstablename + " CASCADE;")
#     cur.execute("CREATE TABLE " + ratingstablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")

#     # Load data from file
#     with open(ratingsfilepath, 'r') as f:
#         cur.copy_from(f, ratingstablename, sep=':')

#     # Remove extra columns
#     cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp;")
#     cur.close()
#     con.commit()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    
    # Drop existing range partition tables if they exist
    for i in range(numberofpartitions):
        table_name = 'range_part' + str(i)
        cur.execute("DROP TABLE IF EXISTS " + table_name + " CASCADE;")
    
    # Calculate delta for uniform ranges
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        
        # Create partition table
        cur.execute("CREATE TABLE " + table_name + " (userid integer, movieid integer, rating float);")
        
        # Insert data based on range
        if i == 0:
            # First partition includes the lower bound
            cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating >= " + str(minRange) + " AND rating <= " + str(maxRange) + ";")
        else:
            # Other partitions exclude the lower bound
            cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating > " + str(minRange) + " AND rating <= " + str(maxRange) + ";")
    
    cur.close()
    con.commit()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    
    # Drop existing round robin partition tables if they exist
    for i in range(numberofpartitions):
        table_name = 'rrobin_part' + str(i)
        cur.execute("DROP TABLE IF EXISTS " + table_name + " CASCADE;")
    
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        
        # Create partition table
        cur.execute("CREATE TABLE " + table_name + " (userid integer, movieid integer, rating float);")
        
        # Insert data using round robin approach
        cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rnum FROM " + ratingstablename + ") as temp WHERE mod(temp.rnum-1, " + str(numberofpartitions) + ") = " + str(i) + ";")
    
    cur.close()
    con.commit()


# def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
#     """
#     Function to insert a new row into the main table and specific partition based on round robin
#     approach.
#     """
#     con = openconnection
#     cur = con.cursor()
#     RROBIN_TABLE_PREFIX = 'rrobin_part'
    
#     # Insert into main table
#     cur.execute("INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    
#     # Get total number of rows in main table
#     cur.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
#     total_rows = (cur.fetchall())[0][0]
    
#     # Get number of partitions
#     numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    
#     # Calculate which partition to insert into
#     index = (total_rows - 1) % numberofpartitions
#     table_name = RROBIN_TABLE_PREFIX + str(index)
    
#     # Insert into appropriate partition
#     cur.execute("INSERT INTO " + table_name + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    
#     cur.close()
#     con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)

    # Tạo metadata nếu chưa có
    cur.execute("""
        CREATE TABLE IF NOT EXISTS roundrobin_metadata (
            insert_count INTEGER
        );
    """)
    cur.execute("SELECT COUNT(*) FROM roundrobin_metadata;")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO roundrobin_metadata (insert_count) VALUES (0);")

    # Lấy lượt chèn hiện tại
    cur.execute("SELECT insert_count FROM roundrobin_metadata;")
    insert_count = cur.fetchone()[0]
    index = insert_count % numberofpartitions

    # Insert vào bảng chính
    cur.execute("INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (%s, %s, %s);",
                (userid, itemid, rating))

    # Insert vào bảng phân mảnh tương ứng
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("INSERT INTO " + table_name + "(userid, movieid, rating) VALUES (%s, %s, %s);",
                (userid, itemid, rating))

    # Tăng lượt chèn
    cur.execute("UPDATE roundrobin_metadata SET insert_count = insert_count + 1;")

    cur.close()
    con.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Insert into main table first
    cur.execute("INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    
    # Get number of partitions
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    
    # Calculate which partition to insert into
    delta = 5.0 / numberofpartitions
    
    # Handle edge case for rating = 0
    if rating == 0:
        index = 0
    else:
        index = int(rating / delta)
        # Handle the case where rating is exactly on a boundary (except 0)
        if rating == (index * delta) and index > 0:
            index = index - 1
        # Ensure index doesn't exceed partition count
        if index >= numberofpartitions:
            index = numberofpartitions - 1
    
    table_name = RANGE_TABLE_PREFIX + str(index)
    
    # Insert into appropriate partition
    cur.execute("INSERT INTO " + table_name + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    
    cur.close()
    con.commit()


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
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()
    
    return count