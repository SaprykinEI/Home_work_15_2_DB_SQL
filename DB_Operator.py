import os

from decimal import Decimal
from datetime import date, datetime

import pyodbc
from dotenv import load_dotenv

import json

import SQL_Queries



class ConnectDB:

    @classmethod
    def connect_to_db(cls, driver, server, pad_database, user, password):
        connection_string = f"""DRIVER={driver};
                                SERVER={server};
                                DATABASE={pad_database};
                                UID={user};
                                PWD={password}"""
        try:
            conn = pyodbc.connect(connection_string)
            conn.autocommit = True
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            return conn

    @classmethod
    def close_connection(cls, conn_obj):
        conn_obj.close()

class HospitalDB:
    def __init__(self, conn_obj):
        self.conn = conn_obj
        self.conn_cursor = self.conn.cursor()

    def check_queries(self, database_name, queries, filename=None):
        """Универсальный метод запросов к базе данных"""

        self.conn_cursor.execute(f'USE {database_name}')
        try:
            query = queries
            self.conn_cursor.execute(query)
            records = self.conn_cursor.fetchall()

            column_names = [desc[0] for desc in self.conn_cursor.description]

            data_list = []
            for record in records:
                data_dict = {
                    column_names[i]: self.convert_value(record[i])
                    for i in range(len(column_names))
                }
                data_list.append(data_dict)

            if filename:
                with open(filename, 'a', encoding='utf-8') as file:
                    json.dump(data_list, file, ensure_ascii=False, indent=4)

                print(f"Данные успешно сохранены в файл {filename}")
            else:
                print("Имя файла не указано")

        except pyodbc.Error as ex:
            print(f"Ошибка при запросе к базе данных: {ex}")

        finally:
            self.conn_cursor.close()

    def convert_value(self, value):
        """Конвертор значений БД"""
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return value


if __name__ == '__main__':
    load_dotenv()

    DRIVER = os.getenv('MS_SQL_DRIVER')
    SERVER = os.getenv('MS_SQL_SERVER')
    WORK_DATABASE = "Hospital"
    PAD_DATABASE = os.getenv('MS_PAD_DATABASE')
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')

    my_conn = ConnectDB.connect_to_db(driver=DRIVER, server=SERVER, pad_database=PAD_DATABASE, user=USER,
                                      password=PASSWORD)
    my_db_operator = HospitalDB(my_conn)
    # my_db_operator.check_queries(WORK_DATABASE, r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_exist_2(),r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_any(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_some(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_all(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_all_any(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_union(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_union_all(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_inner_join(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_left_join(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_right_join(), r'file/data.json')
    # my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_left_right_join(), r'file/data.json')
    my_db_operator.check_queries(WORK_DATABASE, SQL_Queries.queries_full_join(), r'file/data.json')

