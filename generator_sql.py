import textwrap

import pyodbc
import google.generativeai as genai
import os
from dotenv import load_dotenv
import logging
import textwrap
import time
from db_connection import ConnectDB

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Инициализация ключа API для Google Gemini
load_dotenv()
genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))


class SQLQueryGenerator:

    def __init__(self, conn_obj):
        self.conn = conn_obj
        if self.conn:
            self.conn_cursor = self.conn.cursor()
        else:
            self.conn_cursor = None  # Handle case where connection is None

    @staticmethod
    def generate_sql_query(user_query, table_info):
        """
        Генерирует SQL-запрос на основе введенного пользователем текста.
        """

        # Форматируем информацию о таблице для подсказки
        table_info_str = "\n".join(
            [f"Таблица: {table}, Столбцы: {', '.join(columns)}" for table, columns in table_info.items()])

        prompt = f"""
            Ты - эксперт в написании SQL-запросов для Microsoft SQL Server (T-SQL).
            Пользователь хочет выполнить следующую операцию:
            {user_query}

            Даны таблицы и их столбцы:
            {table_info_str}

            Сгенерируй ОДИН SQL-запрос для выполнения этой операции.
            В ответе должен быть ТОЛЬКО SQL-запрос, без пояснений и лишнего текста и каких-либо обрамляющих символов.
            """

        # Отправляем запрос в Google Gemini для генерации SQL
        try:
            model = genai.GenerativeModel('gemini-1.5-pro-latest')  # Укажите модель
            response = model.generate_content(
                prompt)  # Убрал prompt=, так как имя аргумента является первым аргументом generate_content.
            sql_query = response.text.strip()

            # **УДАЛЕНИЕ ОБРАТНЫХ АПОСТРОФОВ**
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

            if sql_query:
                return sql_query
            else:
                logging.warning("Gemini вернул пустой SQL-запрос.")
                return None  # Или вернуть конкретное сообщение об ошибке
        except Exception as e:
            logging.error(f"Ошибка во время генерации SQL: {e}")
            return None

    def execute_sql_query(self, sql_query):
        """Выполняет сгенерированный SQL-запрос и возвращает результаты."""
        if not self.conn or not self.conn_cursor:
            logging.error("Нет доступного соединения с базой данных для выполнения запроса.")
            return None

        try:
            self.conn_cursor.execute(sql_query)
            results = self.conn_cursor.fetchall()
            return results
        except pyodbc.Error as ex:
            logging.error(f"Ошибка при выполнении SQL-запроса: {ex}")
            return None

    import textwrap

    def create_query_function(self, query, name):
        # Формируем строку с запросом внутри функции, начиная с нулевого отступа
        function_code = f"def {name}():\n"  # Добавляем строку с названием функции
        function_code += f"    QUERY = '''{query}'''\n"  # Строка запроса с правильным отступом
        function_code += f"    return QUERY\n"  # Возвращаем результат с нужным отступом

        # Записываем функцию в файл
        with open("SQL_Queries.py", "a", encoding='utf-8') as file:
            file.write(function_code)
