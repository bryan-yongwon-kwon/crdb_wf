import threading
import time
import random
import string
import typer
from psycopg2.pool import ThreadedConnectionPool
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_env import setup_env

app = typer.Typer()
logger = Logger()

@app.command()
def generate_traffic(deployment_env, region, cluster_name, min_conn, max_conn,
                     insert_threads_count, insert_group_count, inserts_per_group, sleep_between_insert_group,
                     delete_threads_count, delete_group_count, deletes_per_group, sleep_between_delete_group):
    min_conn = int(min_conn)
    max_conn = int(max_conn)
    insert_threads_count = int(insert_threads_count)
    insert_group_count = int(insert_group_count)
    inserts_per_group = int(inserts_per_group)
    sleep_between_insert_group = int(sleep_between_insert_group)
    delete_threads_count = int(delete_threads_count)
    delete_group_count = int(delete_group_count)
    deletes_per_group = int(deletes_per_group)
    sleep_between_delete_group = int(sleep_between_delete_group)
    
    setup_env(deployment_env, region, cluster_name)
    crdb_conn = CrdbConnection.get_crdb_connection(cluster_name)
    conn_pool = crdb_conn.get_connection_pool(min_conn, max_conn)
    logger.info("Connection pool created.")
    create_table(conn_pool)
    logger.info("Table 'test' created.")
    threads_list = []
    for t_index in range(insert_threads_count):
        thread = threading.Thread(target=insert_traffic, args=(conn_pool, insert_group_count, inserts_per_group, sleep_between_insert_group))
        thread.start()
        threads_list.append(thread)
        logger.info("Started insert thread {}".format(t_index))

    for t_index in range(delete_threads_count):
        thread = threading.Thread(target=delete_traffic, args=(conn_pool, delete_group_count, deletes_per_group, sleep_between_delete_group))
        thread.start()
        threads_list.append(thread)
        logger.info("Started delete thread {}".format(t_index))

    for thread in threads_list:
        thread.join()
    logger.info("Traffic completed.")

def insert_traffic(conn_pool: ThreadedConnectionPool, insert_group_count: int, inserts_per_group: int, sleep_between_insert_group: int):
    for group in range(insert_group_count):
        for insert in range(inserts_per_group):
            insert_row(conn_pool)
        time.sleep(sleep_between_insert_group)

def delete_traffic(conn_pool: ThreadedConnectionPool, delete_group_count: int, deletes_per_group: int, sleep_between_delete_group: int):
    for group in range(delete_group_count):
        for delete in range(deletes_per_group):
            delete_row(conn_pool)
        time.sleep(sleep_between_delete_group)
    

def create_table(conn_pool: ThreadedConnectionPool):
    conn = conn_pool.getconn()
    try:
        cursor = conn.cursor()
        create_table_sql = """CREATE TABLE IF NOT EXISTS test (
            id UUID NOT NULL DEFAULT gen_random_uuid(),
            name STRING NULL,
            CONSTRAINT "primary" PRIMARY KEY (id ASC),
            Index(name));"""
        cursor.execute(create_table_sql)
        conn.commit()
    except Exception:
        raise Exception("Test table creation failed!")
    finally:
        conn_pool.putconn(conn)

def insert_row(conn_pool: ThreadedConnectionPool):
    conn = conn_pool.getconn()
    try: 
        cursor = conn.cursor()
        name = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(10))
        sql = "INSERT INTO test(name) VALUES ('{}');".format(name)
        cursor.execute(sql)
        conn.commit()
    except:
        pass
    finally:
        conn_pool.putconn(conn)

def delete_row(conn_pool: ThreadedConnectionPool):
    conn = conn_pool.getconn()
    try:
        select_sql = "SELECT id FROM test LIMIT 1;"
        cursor = conn.cursor()
        cursor.execute(select_sql)
        response = cursor.fetchall()
        id = response[0][0]
        delete_sql = "DELETE FROM test WHERE id = '{}';".format(id)
        cursor.execute(delete_sql)
        conn.commit()
    except:
        pass
    finally:
        conn_pool.putconn(conn)