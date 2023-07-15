import mysql.connector
from mysql.connector import errorcode

from utils import read_config, create_database

DB_NAME = 'dannys_diner'

config = read_config(config_file = "../config.ini")

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, DB_NAME)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)


TABLES = {}
TABLES['sales'] = (
    "CREATE TABLE `sales` ("
    "  `customer_id` VARCHAR(1),"
    "  `order_date` DATE,"
    "  `product_id` INT"
    ") ENGINE=InnoDB")

TABLES['menu'] = (
    "CREATE TABLE `menu` ("
    "  `product_id` INT,"
    "  `product_name` varchar(5),"
    "  `price` INT"
    ") ENGINE=InnoDB")

TABLES['members'] = (
    "CREATE TABLE `members` ("
    "  `customer_id` VARCHAR(1),"
    "  `join_date` DATE"
    ") ENGINE=InnoDB")

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


insert_queries = [("INSERT INTO sales (`customer_id`, `order_date`, `product_id`) VALUES (%s, %s, %s)"), 
                    ("INSERT INTO menu (`product_id`, `product_name`, `price`) VALUES (%s, %s, %s)"), 
                    ("INSERT INTO members (`customer_id`, `join_date`) VALUES (%s, %s)")]


data_queries = [[('A', '2021-01-01', '1'),
                 ('A', '2021-01-01', '2'),
                 ('A', '2021-01-07', '2'),
                 ('A', '2021-01-10', '3'),
                 ('A', '2021-01-11', '3'),
                 ('A', '2021-01-11', '3'),
                 ('B', '2021-01-01', '2'),
                 ('B', '2021-01-02', '2'),
                 ('B', '2021-01-04', '1'),
                 ('B', '2021-01-11', '1'),
                 ('B', '2021-01-16', '3'),
                 ('B', '2021-02-01', '3'),
                 ('C', '2021-01-01', '3'),
                 ('C', '2021-01-01', '3'),
                 ('C', '2021-01-07', '3')],
                [('1', 'sushi', '10'),
                 ('2', 'curry', '15'),
                 ('3', 'ramen', '12')],
                [('A', '2021-01-07'),
                ('B', '2021-01-09')]]

try:
    for query in range(len(insert_queries)):
        for data in data_queries[query]:
            cursor.execute(insert_queries[query], data)
        cnx.commit()
        print("Data inserted successfully.")

except:
    print("Error inserting data: {}".format(err))
    cnx.rollback()


cursor.close()
cnx.close()