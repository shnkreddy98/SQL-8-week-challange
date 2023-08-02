import mysql.connector
from mysql.connector import errorcode

from utils import read_config, create_database

DB_NAME = 'pizza_runner'

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

TABLES['runners'] = (
        "CREATE TABLE `runners` ("
        "`runner_id` INTEGER, "
        "`registration_date` DATE"
        ") ENGINE=InnoDB")

TABLES['customer_orders'] = (
        "CREATE TABLE `customer_orders` ("
        "`order_id` INTEGER,"
        "`customer_id` INTEGER,"
        "`pizza_id` INTEGER,"
        "`exclusions` VARCHAR(4),"
        "`extras` VARCHAR(4),"
        "`order_time` TIMESTAMP"
        ") ENGINE=InnoDB")

TABLES['runner_orders'] = (
        "CREATE TABLE `runner_orders` ("
        "`order_id` INTEGER,"
        "`runner_id` INTEGER,"
        "`pickup_time` VARCHAR(19),"
        "`distance` VARCHAR(7),"
        "`duration` VARCHAR(10),"
        "`cancellation` VARCHAR(23)"
        ") ENGINE=InnoDB")

TABLES['pizza_names'] = (
        "CREATE TABLE `pizza_names` ("
        "`pizza_id` INTEGER,"
        "`pizza_name` TEXT"
        ") ENGINE=InnoDB")

TABLES['pizza_recipes'] = (
        "CREATE TABLE `pizza_recipes` ("
        "`pizza_id` INTEGER,"
        "`toppings` TEXT"
        ") ENGINE=InnoDB")

TABLES['pizza_toppings'] = (
            "CREATE TABLE `pizza_toppings` ("
            "`topping_id` INTEGER,"
            "`topping_name` TEXT"
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


insert_queries = [("INSERT INTO runners (`runner_id`, `registration_date`) VALUES (%s, %s)"), 
                  ("INSERT INTO customer_orders (`order_id`, `customer_id`, `pizza_id`, `exclusions`, `extras`, `order_time`) VALUES (%s, %s, %s, %s, %s, %s)"),
                  ("INSERT INTO runner_orders (`order_id`, `runner_id`, `pickup_time`, `distance`, `duration`, `cancellation`) VALUES (%s, %s, %s, %s, %s, %s)"),
                  ("INSERT INTO pizza_names (`pizza_id`, `pizza_name`) VALUES (%s, %s)"),
                  ("INSERT INTO pizza_recipes (`pizza_id`, `toppings`) VALUES (%s, %s)"),
                  ("INSERT INTO pizza_toppings (`topping_id`, `topping_name`) VALUES (%s, %s)")]


data_queries = [[(1, '2021-01-01'),
                 (2, '2021-01-03'),
                 (3, '2021-01-08'),
                 (4, '2021-01-15')],
                [('1', '101', '1', '', '', '2020-01-01 18:05:02'),
                 ('2', '101', '1', '', '', '2020-01-01 19:00:52'),
                 ('3', '102', '1', '', '', '2020-01-02 23:51:23'),
                 ('3', '102', '2', '', 'Null', '2020-01-02 23:51:23'),
                 ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
                 ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
                 ('4', '103', '2', '4', '', '2020-01-04 13:23:46'),
                 ('5', '104', '1', 'null', '1', '2020-01-08 21:00:29'),
                 ('6', '101', '2', 'null', 'null', '2020-01-08 21:03:13'),
                 ('7', '105', '2', 'null', '1', '2020-01-08 21:20:29'),
                 ('8', '102', '1', 'null', 'null', '2020-01-09 23:54:33'),
                 ('9', '103', '1', '4', '1, 5', '2020-01-10 11:22:59'),
                 ('10', '104', '1', 'null', 'null', '2020-01-11 18:34:49'),
                 ('10', '104', '1', '2, 6', '1, 4', '2020-01-11 18:34:49')],
                [('1', '1', '2020-01-01 18:15:34', '20km', '32 minutes', ''),
                 ('2', '1', '2020-01-01 19:10:54', '20km', '27 minutes', ''),
                 ('3', '1', '2020-01-03 00:12:37', '13.4km', '20 mins', 'NULL'),
                 ('4', '2', '2020-01-04 13:53:03', '23.4', '40', 'NULL'),
                 ('5', '3', '2020-01-08 21:10:57', '10', '15', 'NULL'),
                 ('6', '3', 'null', 'null', 'null', 'Restaurant Cancellation'),
                 ('7', '2', '2020-01-08 21:30:45', '25km', '25mins', 'null'),
                 ('8', '2', '2020-01-10 00:15:02', '23.4 km', '15 minute', 'null'),
                 ('9', '2', 'null', 'null', 'null', 'Customer Cancellation'),
                 ('10', '1', '2020-01-11 18:50:20', '10km', '10minutes', 'null')],
                [(1, 'Meatlovers'),
                 (2, 'Vegetarian')],
                [(1, '1, 2, 3, 4, 5, 6, 8, 10'),
                 (2, '4, 6, 7, 9, 11, 12')],
                [(1, 'Bacon'),
                 (2, 'BBQ Sauce'),
                 (3, 'Beef'),
                 (4, 'Cheese'),
                 (5, 'Chicken'),
                 (6, 'Mushrooms'),
                 (7, 'Onions'),
                 (8, 'Pepperoni'),
                 (9, 'Peppers'),
                 (10, 'Salami'),
                 (11, 'Tomatoes'),
                 (12, 'Tomato Sauce')]]


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