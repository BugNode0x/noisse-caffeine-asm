import psycopg2
import re
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

def is_valid_domain(domain_name):
    # Regular expression for validating a domain name
    pattern = r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$"
    return re.match(pattern, domain_name) is not None

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
        if commit:
            conn.commit()
        if fetch_one:
            return cur.fetchone()
        return None
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

def ensure_user_domain_exists(user_id, domain_id):
    # Check if the user-domain pair exists
    query = 'SELECT id FROM user_domains WHERE user_id = %s AND domain_id = %s'
    existing_id = execute_db_query(query, (user_id, domain_id), fetch_one=True)
    if existing_id is None:
        # Insert the new user-domain pair
        insert_query = 'INSERT INTO user_domains (user_id, domain_id) VALUES (%s, %s)'
        execute_db_query(insert_query, (user_id, domain_id), commit=True)

def ensure_domain_exists(domain_name):
    existing_domain_id = get_root_domain_id(domain_name)
    if existing_domain_id:
        return existing_domain_id
    # Insert new domain
    insert_query = 'INSERT INTO domains (domain_name) VALUES (%s)'
    execute_db_query(insert_query, (domain_name,), commit=True)
    return get_root_domain_id(domain_name)

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
    user_id = get_user_id_from_hunter_id(hunter_id)
    domain_id = ensure_domain_exists(domain)
    ensure_user_domain_exists(user_id, domain_id)

    for subdomain in subdomains:
        subdomain_id = execute_db_query("SELECT subdomain_id FROM subdomains WHERE subdomain = %s", (subdomain,), fetch_one=True)
        if subdomain_id is None:
            print(f"Inserting new subdomain: {subdomain}")
            subdomain_id = execute_db_query("INSERT INTO subdomains (subdomain) VALUES (%s) RETURNING subdomain_id", (subdomain,), fetch_one=True, commit=True)

        if subdomain_id:
            execute_db_query("INSERT INTO user_subdomain (user_id, subdomain_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, subdomain_id), commit=True)
        else:
            print(f"Failed to insert subdomain: {subdomain}")