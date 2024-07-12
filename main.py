import json
import random
from faker import Faker
import snowflake.connector
import time

fake = Faker()

categories = [
    {'id': 1, 'name': 'Ice Cream', 'subcategories': ['Freezing Point', 'Melting Point']},
    {'id': 2, 'name': 'Drinks', 'subcategories': ['Beverage', 'Alcohol']}
]

item_types = ['Dessert', 'Beverage']
temperatures = ['Cold Option', 'Hot Option']

def generate_menu_item(category, subcategory):
    menu_item_id = fake.random_int(min=10, max=1000)
    menu_item_name = fake.word().title()
    menu_item_type = random.choice(item_types)
    temperature = random.choice(temperatures)
    cost = round(random.uniform(0.5, 5.0), 4)
    price = round(cost * random.uniform(1.5, 3.0), 4)
    details = {
        "menu_item_health_metrics": [
            {
                "ingredients": [fake.word() for _ in range(random.randint(2, 5))],
                "is_dairy_free_flag": random.choice(["Y", "N"]),
                "is_gluten_free_flag": random.choice(["Y", "N"]),
                "is_healthy_flag": random.choice(["Y", "N"]),
                "is_nut_free_flag": random.choice(["Y", "N"])
            }
        ],
        "menu_item_id": menu_item_id
    }
    return (
        fake.random_int(min=10000, max=99999),
        category['id'],
        category['name'],
        subcategory,
        menu_item_id,
        menu_item_name,
        menu_item_type,
        temperature,
        cost,
        price,
        json.dumps(details)
    )


def query_data(cur):
    select_query = "SELECT * FROM demo.raw_pos.menu"
    cur.execute(select_query)
    rows = cur.fetchall()
    # for row in rows:
    #     print(row)

def delete_data(cur):
    delete_query = "DELETE FROM demo.raw_pos.menu"
    cur.execute(delete_query)



if __name__ == '__main__':
    # Snowflake connection details
    account = 'sxfnyvr-qbb59115'
    user = 'insightfinder'
    password = 'InsightFinder@01'

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
    )

    # Create a cursor object
    cur = conn.cursor()

    # Create demo database and table
    cur.execute("USE ROLE accountadmin;")
    cur.execute("USE WAREHOUSE compute_wh;")
    cur.execute("CREATE OR REPLACE DATABASE demo;")
    cur.execute("CREATE OR REPLACE SCHEMA demo.raw_pos;")
    cur.execute("""
    CREATE OR REPLACE TABLE demo.raw_pos.menu
    (
        menu_id NUMBER(19,0),
        menu_type_id NUMBER(38,0),
        menu_type VARCHAR(16777216),
        truck_brand_name VARCHAR(16777216),
        menu_item_id NUMBER(38,0),
        menu_item_name VARCHAR(16777216),
        item_category VARCHAR(16777216),
        item_subcategory VARCHAR(16777216),
        cost_of_goods_usd NUMBER(38,4),
        sale_price_usd NUMBER(38,4),
        menu_item_health_metrics_obj VARCHAR(16777216)
    );
    """)

    while True:
        print("Inserting data...")
        for _ in range(100):
            # Generate fake data
            category = random.choice(categories)
            subcategory = random.choice(category['subcategories'])
            data = generate_menu_item(category, subcategory)

            # Insert data
            cur.execute(f"INSERT INTO demo.raw_pos.menu VALUES {data}")

        print("Querying data...")
        query_data(cur)

        print("Deleting data...")
        delete_data(cur)
