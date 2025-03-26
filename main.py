# main.py
import os
from dotenv import load_dotenv
import logging
import pyodbc
from db_connection import ConnectDB
from generator_sql import SQLQueryGenerator  # Обновленный импорт
from HospitalDB import HospitalDB
import SQL_Queries

"""ПРОГРАММА 1-я ее часть пишите запрос словами она переводит его в SQL-запрос, и записывает в файл SQL_Queries как функцию
ВАЖНО писать данные которые действительно есть в таблицах, еще не все настроил проверки.

Вторая часть программы обрабатывает запрос из файла SQL_queries и сохраняет данные в json, что бы не комментить каждый раз части программ поставил флаг
flag == True (работает первая часть) flag == False вторая часть
И запросы во второй части пока что нужно подкидывать вручную
"""

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == '__main__':
    load_dotenv()

    DRIVER = os.getenv('MS_SQL_DRIVER')
    SERVER = os.getenv('MS_SQL_SERVER')
    WORK_DATABASE = "Hospital"
    PAD_DATABASE = os.getenv('MS_PAD_DATABASE')  # USE THIS ONE TO CONNECT!
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')

    my_conn = ConnectDB.connect_to_db(driver=DRIVER, server=SERVER, pad_database=PAD_DATABASE, user=USER,
                                      password=PASSWORD)
    flags = True

    if flags == True:
        if my_conn:
            try:

                table_info = ConnectDB.get_table_info(my_conn)

                if table_info:
                    print("Напишите запрос словами: Пример 'Имена и фамилии врачей и их зарплата'" )
                    user_query = input("Введите запрос словами: ")
                    user_query_func = input("Введите название функции (например salary_doctors): ")
                    my_sql = SQLQueryGenerator(my_conn)
                    sql_query = my_sql.generate_sql_query(user_query, table_info)

                    logging.info(f"Сгенерированный SQL-запрос: {sql_query}")
                    print()

                    save_sql = SQLQueryGenerator(my_conn)
                    save_sql.create_query_function(sql_query, user_query_func)

                    if sql_query:
                        results = my_sql.execute_sql_query(sql_query)

                        if results:
                            logging.info("Результаты выполнения SQL-запроса:")
                            for row in results:
                                logging.info(row)
                        else:
                            logging.info("Выполнение SQL-запроса не дало результатов.")
                    else:
                        logging.error("Не удалось сгенерировать SQL-запрос.")
            except Exception as e:
                logging.error(f"Произошла ошибка при обработке запроса: {e}")

            finally:
                if my_conn:
                    ConnectDB.close_connection(my_conn)
        else:
            logging.error("Не удалось подключиться к базе данных.")
    if flags == False:
        #Пример вызова SQL - запроса из SQL_Queries
        my_db_operator = HospitalDB(my_conn)
        full_join_query = SQL_Queries.doc_otd()
        my_db_operator.check_queries(WORK_DATABASE, full_join_query, r'file/data.json')

