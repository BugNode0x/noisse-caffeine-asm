import psycopg2
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

def create_db_connection():
    try:
        return psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database connection error: {error}")
        return None

def execute_db_query(query, params, fetch_one=False, commit=False):
    conn = create_db_connection()
    if conn is None:
        return None

    try:
        cur = conn.cursor()
        cur.execute(query, params)
        if fetch_one:
            result = cur.fetchone()
            return result[0] if result else None
        if commit:
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database query error: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()

def ensure_hunter_exists(hunter_id):
    existing_hunter_id = get_hunter_id(hunter_id)
    if existing_hunter_id is None:
        query = 'INSERT INTO users (hunter_id) VALUES (%s)'
        execute_db_query(query, (hunter_id,), commit=True)

def ensure_domain_exists(domain_name, hunter_id):
    existing_domain_id = get_root_domain_id(domain_name)
    if existing_domain_id is None:
        user_id = get_user_id_from_hunter_id(hunter_id)  # Fetch user_id based on hunter_id
        if user_id is None:
            print(f"Unable to find user_id for hunter_id {hunter_id}")
            return
        query = 'INSERT INTO domains (domain_name, user_id) VALUES (%s, %s)'
        execute_db_query(query, (str(domain_name), user_id), commit=True)

def get_user_id_from_hunter_id(hunter_id):
    query = 'SELECT user_id FROM users WHERE hunter_id = %s'
    return execute_db_query(query, (hunter_id,), fetch_one=True)


def get_hunter_id(hunter_id):
    query = 'SELECT hunter_id FROM users WHERE hunter_id = %s'
    return execute_db_query(query, (hunter_id,), fetch_one=True, commit=True)

def get_root_domain_id(domain_name):
    query = 'SELECT domain_id FROM domains WHERE domain_name = %s'
    return execute_db_query(query, (str(domain_name),), fetch_one=True)


def insert_subdomain_results(hunter_id, domain, subdomains):
    ensure_hunter_exists(hunter_id)
    ensure_domain_exists(domain, hunter_id)
    
    root_domain_id = get_root_domain_id(domain)
    if root_domain_id is None:
        print(f"Error: Unable to find root_domain_id for domain {domain}")
        return

    for subdomain in subdomains:
        query = 'INSERT INTO subdomain_results (hunter_id, root_domain_id, subdomain) VALUES (%s, %s, %s)'
        execute_db_query(query, (hunter_id, root_domain_id, subdomain), commit=True)
