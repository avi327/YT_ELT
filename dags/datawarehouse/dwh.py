from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor,create_schema, create_table,get_video_ids
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_loading import load_data
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    schema = "staging"

    conn,cur = None, None
    try:
        conn, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema)
        YT_data = load_data()
        table_ids = get_video_ids(cur, schema)
        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
        ids_to_json = {row["video_id"] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_to_json
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        logger.info(f"'{schema}' updated successfully.")
    except Exception as e:
        logger.error(f"an error occurred while updating of {schema}: table: {e}")
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
@task
def core_table():
    schema = "core"

    conn,cur = None, None      
    try:
            conn, cur = get_conn_cursor()
            create_schema(schema)
            create_table(schema)
            table_ids = get_video_ids(cur, schema)   
            current_video_ids = set()
            cur.execute(f"select * from staging.{table};")
            rows = cur.fetchall()
            for row in rows:
                if len(table_ids) == 0:
                    transformed_row = transform_data(row)
                    insert_rows(cur, conn, schema, transformed_row)
                else:
                    transformed_row = transform_data(row)
                    if row["Video_ID"] in table_ids:
                        update_rows(cur, conn, schema, transformed_row)
                    else:
                        insert_rows(cur, conn, schema, transformed_row)

            ids_to_delete = set(table_ids) - current_video_ids
            if ids_to_delete:
                    delete_rows(cur, conn, schema, ids_to_delete)
            logger.info(f"'{schema}' updated successfully.")
    except Exception as e:
        logger.error(f"an error occurred while updating of {schema}: table: {e}")
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()