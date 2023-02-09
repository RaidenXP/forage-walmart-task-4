import csv
import sqlite3

def csv_reading(name):
    path = "data/" + name
    
    data = []

    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    
    return data

def insert_row(row):
    conn = sqlite3.connect('shipment_database.db')
    c = conn.cursor()

    c.execute(
        """
        INSERT INTO shipment_database VALUES 
        (:origin_warehouse, :destination_store, :product, :on_time, :product_quantity, :driver_identifier)
        """,
        row
    )

    conn.commit()
    conn.close()

def check_db():
    conn = sqlite3.connect('shipment_database.db')
    c = conn.cursor()

    c.execute("SELECT * FROM shipment_database")

    print(c.fetchall())

    conn.close()

def initialize():
    data = csv_reading("shipping_data_0.csv")

    conn = sqlite3.connect('shipment_database.db')
    c = conn.cursor()

    c.execute("""CREATE TABLE shipment_database (
            origin_warehouse text,
            destination_store text,
            product text,
            on_time text,
            product_quantity integer,
            driver_identifier text
            )""")

    for row in data:
        c.execute(
            """
            INSERT INTO shipment_database VALUES 
            (:origin_warehouse, :destination_store, :product, :on_time, :product_quantity, :driver_identifier)
            """,
            row
        )

    conn.commit()
    conn.close()

def munging():
    data_1 = csv_reading("shipping_data_1.csv")
    data_2 = csv_reading("shipping_data_2.csv")

    modified_1 = manip_data_1(data_1)
    on_time = on_time_info(data_1)
    modified_2 = manip_data_2(data_2)

    final_data = []

    for shipment in modified_1.keys():
        for product in modified_1[shipment].keys():
            row = {}
            row['origin_warehouse'] = modified_2[shipment]['origin_warehouse']
            row['destination_store'] = modified_2[shipment]['destination_store']
            row['product'] = product
            row['on_time'] = on_time[shipment]
            row['product_quantity'] = modified_1[shipment][product]
            row['driver_identifier'] = modified_2[shipment]['driver_identifier']
            final_data.append(row)

    return final_data

def manip_data_1(data):
    changed = {}
    for row in data:
        if row['shipment_identifier'] not in changed.keys():
            changed[row['shipment_identifier']] = {row['product']: 1}
        elif row['shipment_identifier'] in changed.keys() and row['product'] not in changed[row['shipment_identifier']].keys():
            changed[row['shipment_identifier']][row['product']] = 1
        else:
            changed[row['shipment_identifier']][row['product']] += 1
    
    return changed

def on_time_info(data):
    changed = {}
    for row in data:
        if row['shipment_identifier'] not in changed.keys():
            changed[row['shipment_identifier']] = row['on_time']
    
    return changed

def manip_data_2(data):
    changed = {}
    for row in data:
        changed[row['shipment_identifier']] = row.copy()
        changed[row['shipment_identifier']].pop('shipment_identifier')
    
    return changed

def finalize(moded_data):
    conn = sqlite3.connect('shipment_database.db')
    c = conn.cursor()

    for row in moded_data:
        c.execute(
            """
            INSERT INTO shipment_database VALUES 
            (:origin_warehouse, :destination_store, :product, :on_time, :product_quantity, :driver_identifier)
            """,
            row
        )

    conn.commit()
    conn.close()