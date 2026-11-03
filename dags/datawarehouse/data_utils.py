from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"
def get_conn_cursor():
    """
    Establishes a connection to the PostgreSQL database and returns a cursor.
    
    Returns:
        cursor (RealDictCursor): A cursor object for executing SQL queries.
    """
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt",database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn,cur
def close_conn_cursor(conn, cur):
    """
    Closes the cursor and connection to the PostgreSQL database.
    
    Args:
        conn: The connection object to the PostgreSQL database.
        cur: The cursor object used for executing SQL queries.
    """
    cur.close()
    conn.close()

def create_schema(schema_name):
    """
    Creates a schema in the PostgreSQL database if it does not already exist.
    
    Args:
        schema_name (str): The name of the schema to create.
    """
    conn, cur = get_conn_cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()
    close_conn_cursor(conn, cur)

def create_table(schema):
    """
    Creates a table in the specified schema of the PostgreSQL database if it does not already exist.
    
    Args:
        schema_name (str): The name of the schema where the table will be created.
    """
    conn, cur = get_conn_cursor()
    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_ids(cur,schema):
    """
    Retrieves video IDs from the specified table in the PostgreSQL database.
    
    Args:
        schema (str): The name of the schema where the table is located.
        table (str): The name of the table from which to retrieve video IDs.
    
    Returns:
        list: A list of video IDs retrieved from the specified table.
    """
    cur.execute(f"""select "Video_ID" from {schema}.{table};""")
    ids = cur.fetchall()
    video_ids = [row["Video_ID"] for row in ids]
    return video_ids