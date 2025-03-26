# main.py
import os
from dotenv import load_dotenv
import logging
import pyodbc
from db_connection import ConnectDB
from generator_sql import SQLQueryGenerator  # Обновленный импорт
from HospitalDB import HospitalDB
import SQL_Queries

# Configure logging
logging.basicConfig(level=logging.INFO, format='\t%(asctime)s - %(levelname)s - %(message)s')


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


    def main_menu():
        print("Здравствуйте! Вас приветствует программа 'QueryMaster'.\n")
        while True:
            print("\nМеню:")
            print("1. Получить данные из таблицы")
            print("2. Просмотреть все запросы")
            print("3. Выход")

            choice = input("Выберите пункт меню: ")

            if choice == '1':

                if my_conn:
                    try:

                        table_info = ConnectDB.get_table_info(my_conn)

                        if table_info:
                            user_query = input("Введите запрос словами: ")
                            my_sql = SQLQueryGenerator(my_conn)
                            sql_query = my_sql.generate_sql_query(user_query, table_info)

                            logging.info(f"Сгенерированный SQL-запрос: {sql_query}")

                            save_sql = SQLQueryGenerator(my_conn)
                            save_sql.create_query_function(sql_query)

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
            elif choice == '2':
                # Показать все доступные запросы из SQL_Queries
                print("\nДоступные запросы:")
                for i, query in enumerate(SQL_Queries.queries_list(), 1):
                    print(f"{i}. {query['description']}")

                query_choice = int(input("\nВыберите запрос для выполнения: "))
                query = SQL_Queries.queries_list()[query_choice - 1]
                sql_query = query['query']
                print(f"\nВыполняем запрос: {sql_query}")

if __name__ == "__main__":
    main_menu()