import time
import psycopg2
import random
from faker import Faker
from datetime import datetime, UTC

DB_CONFIG = {
    "dbname": "fraud_detection",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432,
}

fake = Faker()

CREATE_First_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS transactions (
    trans_date_trans_time TIMESTAMP WITHOUT TIME ZONE,
    cc_num BIGINT,
    merchant VARCHAR(255),
    category VARCHAR(255),
    amt DOUBLE PRECISION,
    first VARCHAR(255),
    last VARCHAR(255),
    gender VARCHAR(10),
    street VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(10),
    zip VARCHAR(20),
    lat DOUBLE PRECISION,
    long DOUBLE PRECISION,
    city_pop INT,
    job VARCHAR(255),
    dob DATE,  -- Ensure dob is of type DATE
    trans_num VARCHAR(255),
    unix_time BIGINT,
    merch_lat DOUBLE PRECISION,
    merch_long DOUBLE PRECISION
);
"""

CREATE_Second_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS processed_transactions (
    trans_date_trans_time TIMESTAMP WITHOUT TIME ZONE,
    cc_num BIGINT,
    merchant VARCHAR(255),
    category VARCHAR(255),
    amt DOUBLE PRECISION,
    full_name VARCHAR(510),  
    gender VARCHAR(10),
    street VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(10),
    zip VARCHAR(20),
    lat DOUBLE PRECISION,
    long DOUBLE PRECISION,
    city_pop INT,
    job VARCHAR(255),
    dob DATE,  -- Ensure dob is of type DATE
    trans_num VARCHAR(255),
    unix_time BIGINT,
    merch_lat DOUBLE PRECISION,
    merch_long DOUBLE PRECISION,
    is_fraud int
);
"""

INSERT_QUERY = """
INSERT INTO transaction (
    trans_date_trans_time, cc_num, merchant, category, amt, first, last, gender, 
    street, city, state, zip, lat, long, city_pop, job, dob, trans_num, 
    unix_time, merch_lat, merch_long
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""


def generate_random_transaction():
    trans_time = datetime.now(UTC)
    unix_time = int(trans_time.timestamp())
    return (
        trans_time,
        fake.random_int(min=4000000000000000, max=4999999999999999),
        fake.company(),
        fake.random_element(["grocery", "electronics", "clothing", "fuel", "restaurant"]),
        round(random.uniform(1, 500), 2),
        fake.first_name(),
        fake.last_name(),
        fake.random_element(["M", "F"]),
        fake.street_address(),
        fake.city(),
        fake.state_abbr(),
        fake.zipcode_in_state(),
        round(fake.latitude(), 6),
        round(fake.longitude(), 6),
        random.randint(500, 1000000),
        fake.job(),
        fake.date_object(),
        fake.uuid4(),
        unix_time,
        round(fake.latitude(), 6),
        round(fake.longitude(), 6)
    )


def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute(CREATE_First_TABLE_QUERY)

        cursor.execute(CREATE_Second_TABLE_QUERY)

        conn.commit()
        print("Table ensured.")

        while True:
            transaction = generate_random_transaction()
            cursor.execute(INSERT_QUERY, transaction)
            conn.commit()
            print("Inserted transaction:", transaction)
            time.sleep(8)

    except Exception as e:
        print("Error:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()