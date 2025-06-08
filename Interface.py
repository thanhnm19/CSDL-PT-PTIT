import psycopg2
import io
DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='admin123', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    # Xóa bảng nếu đã tồn tại
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
    # Tạo bảng Ratings với 3 cột
    cur.execute(f"CREATE TABLE {ratingstablename} (userid INT, movieid INT, rating FLOAT);")
    
    # Đọc file và chuẩn hóa dữ liệu từ '::' sang tab '\t'
    buffer = io.StringIO()
    with open(ratingsfilepath, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split("::")
            if len(parts) >= 3:
                buffer.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
    buffer.seek(0)

    # Dùng COPY để insert nhanh
    cur.copy_from(buffer, ratingstablename, sep='\t', columns=('userid', 'movieid', 'rating'))
    openconnection.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    
    # Bỏ các bảng phân mảnh hiện có nếu chúng tồn tại
    for i in range(numberofpartitions):
        table_name = 'range_part' + str(i)
        cur.execute("DROP TABLE IF EXISTS " + table_name + " CASCADE;")
    
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        
        # Tạo bảng phân mảnh
        cur.execute("CREATE TABLE " + table_name + " (userid integer, movieid integer, rating float);")
        
        # Insert data 
        if i == 0:
            cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating >= " + str(minRange) + " AND rating <= " + str(maxRange) + ";")
        else:
            cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating > " + str(minRange) + " AND rating <= " + str(maxRange) + ";")
    
    cur.close()
    con.commit()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    
    # Xóa bảng phân mảnh hiện thời nếu nó tồn tại
    for i in range(numberofpartitions):
        table_name = 'rrobin_part' + str(i)
        cur.execute("DROP TABLE IF EXISTS " + table_name + " CASCADE;")
    
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        
        # Tạo bảng phân mảnh
        cur.execute("CREATE TABLE " + table_name + " (userid integer, movieid integer, rating float);")
        
        # Insert data 
        cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rnum FROM " + ratingstablename + ") as temp WHERE mod(temp.rnum-1, " + str(numberofpartitions) + ") = " + str(i) + ";")
    
    cur.close()
    con.commit()


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
    
    # Insert vào bảng chính
    cur.execute("INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    
    #Lấy số phân mảnh hiện có
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    
    # Tính delta
    delta = 5.0 / numberofpartitions
    
    if rating == 0:
        index = 0
    else:
        index = int(rating / delta)
        if rating == (index * delta) and index > 0:
            index = index - 1
        if index >= numberofpartitions:
            index = numberofpartitions - 1
    
    table_name = RANGE_TABLE_PREFIX + str(index)
    
    # Insert vào phân mảnh 
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
