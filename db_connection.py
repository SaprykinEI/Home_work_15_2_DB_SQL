import pyodbc
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ConnectDB:
    @classmethod
    def connect_to_db(cls, driver, server, pad_database, user, password):
        connection_string = f'''DRIVER={driver};
                                SERVER={server};
                                DATABASE={pad_database};
                                UID={user};
                                PWD={password}'''

        try:
            conn = pyodbc.connect(connection_string)
            conn.autocommit = True
            logging.info("Соединение с базой данных установлено.")
            return conn
        except pyodbc.ProgrammingError as ex:
            logging.error(f"Ошибка подключения к базе данных: {ex}")
            return None

    @classmethod
    def close_connection(cls, conn_obj):
        if conn_obj:
            conn_obj.close()
            logging.info("Соединение с базой данных закрыто.")

    @classmethod
    def get_table_info(cls, conn):
        if not conn:
            logging.error("Нет доступного соединения с базой данных.")
            return {}  # Возвращаем пустой словарь, если нет соединения

        cursor = conn.cursor()
        try:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
            tables = cursor.fetchall()

            table_info = {}
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                columns = cursor.fetchall()
                table_info[table_name] = [column[0] for column in columns]

            return table_info
        except pyodbc.Error as ex:
            logging.error(f"Ошибка при получении информации о таблице: {ex}")
            return {}
        finally:
            cursor.close()
