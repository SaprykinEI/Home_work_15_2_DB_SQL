
import os
from decimal import Decimal
from datetime import date, datetime
import pyodbc
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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

