import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Description: This function is responsible for creating the sparkifydb and
    create a conncetion to the same.

    Arguments:
        None

    Returns:
        cur: the cursor object.
        conn: the database connection object.
    """

    # connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Description: This function drops the tables using the queries in the
    `drop_table_queries` list and then commits the changes to the database
    sparkifydb.

    Arguments:
        cur: the cursor object.
        conn: the database connection object.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function creates the tables using the queries in the
    `create_table_queries` list and then commits the changes to the database
    sparkifydb.

    Arguments:
        cur: the cursor object.
        conn: the database connection object.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function first (drops, if exists) creates a connection to the
    sparkifydb database and also generates a cursor object. It then drops the tables
    and creates them again, and finally closes the database connection. This way, it
    is easy to reset the tables before executing the ETL pipeline.

    Arguments:
        None

    Returns:
        None
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
