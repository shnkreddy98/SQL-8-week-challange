import os
from configparser import ConfigParser
import mysql.connector


def read_config(config_file = 'config.ini', section = 'mysql'):
    """
    Read the configuration file config_file with the given section.
    If successful, return the configuration as a dictionary,
    else raise an exception.
    """
    parser = ConfigParser()
    
    # Does the configuration file exist?
    if os.path.isfile(config_file):
        parser.read(config_file)
    else:
        raise Exception(f"Configuration file '{config_file}' "
                        "doesn't exist.")
    
    config = {}
    
    if parser.has_section(section):
        # Parse the configuration file.
        items = parser.items(section)
        
        # Construct the parameter dictionary.
        for item in items:
            config[item[0]] = item[1]
            
    else:
        raise Exception(f'Section [{section}] missing ' + \
                        f'in config file {config_file}')
    
    return config


def create_database(cursor, DB_NAME):
    try:
        cursor.execute(
            "CREATE DATABASE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)