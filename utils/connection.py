import pyodbc as odbc
import configparser

config = configparser.RawConfigParser()
config.read('application.properties')
def set_up_conn():
    DRIVER_NAME = config.get('DatabaseSection', 'database.driver')
    SERVER_NAME = config.get('DatabaseSection', 'database.servername')
    DATABASE_NAME = config.get('DatabaseSection', 'database.dbname')

    return odbc.connect(f'DRIVER={{{DRIVER_NAME}}};'
                        f'SERVER={SERVER_NAME};'
                        f'DATABASE={DATABASE_NAME};'
                        'Trusted_connection=yes;')