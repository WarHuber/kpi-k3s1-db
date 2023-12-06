import psycopg2
from typing import Optional, Tuple, Union, List

class Model:
    def __init__(self, db_name: str, user: str, password: str, host: str):
        """
        This is the constructor method for the class. It initializes the instance variables with the provided values.

        Parameters:
        db_name (str): The name of the PostgreSQL database to connect to.
        user (str): The username used to authenticate with the PostgreSQL server.
        host (str): The host of the PostgreSQL server.
        password (str): The password used to authenticate with the PostgreSQL server.
        """
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host

    def connect(self) -> Tuple[Optional[psycopg2.extensions.connection], Optional[psycopg2.extensions.cursor]]:
        """
        This method is used to establish a connection to the PostgreSQL database.

        It uses the psycopg2 library to create a connection and a cursor object.
        The connection details are taken from the instance variables of the class.

        Returns:
        conn (psycopg2.extensions.connection, optional): The connection object to the database, or None if the connection was not successful.
        cur (psycopg2.extensions.cursor, optional): The cursor object to execute PostgreSQL commands through Python, or None if the connection was not successful.
        """
        try:
            conn = psycopg2.connect(f"dbname='{self.db_name}' user='{self.user}' host='{self.host}' password='{self.password}'")
            cur = conn.cursor()
        except psycopg2.OperationalError as e:
            print("Unable to connect to the database\n", e)
            return None, None

        return conn, cur

    def insert_data(self, table: str, columns: list, data: dict) -> bool:
        """
        This method is used to insert data into a specific table in the database.

        Parameters:
        table (str): The name of the table where the data will be inserted.
        columns (list): A list of column names where the data will be inserted.
        data (dict): A dictionary where the key is the column name and the value is the data to be inserted.

        Returns:
        bool: True if the data was successfully inserted, False otherwise.
        """
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return False

        values = []
        columns_str = ", ".join(columns)
        for key in data:
            values.append(data[key])
        
        try:
            values = tuple(values)
            query = f"INSERT INTO {table} ({columns_str}) VALUES {values}"
            cur.execute(query)
        except Exception as e:
            print("Error: Invalid data insert\n", e)
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True
    
    def get_tables(self) -> Union[list, None]:
        """
        This method is used to retrieve the names of all the tables in the database.

        Returns:
        tables (list or None): A list of strings representing the names of the tables in the database.
        None: If there is an error in connection or execution, or if there are no tables in the database.
        """
        conn, cur = self.connect()

        if conn is None or cur is None:
            return None
        
        try:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            cur.execute(query)
            tables = cur.fetchall()
        except Exception as e:
            print("Error: Invalid tables get\n", e)
            return None

        conn.commit()
        cur.close()
        conn.close()

        # If there are no tables in the database, return "No tables found"
        if len(tables) == 0:
            return None

        return tables

    def get_data(self, table: str, columns: list, condition=None) -> Union[list, None]:
        """
        This method is used to retrieve data from a specific table in the database.

        Parameters:
        table (str): The name of the table from which the data will be retrieved.
        columns (list): The names of the columns to be retrieved.
        condition (str, optional): The condition for the data retrieval. Defaults to None.

        Returns:
        data (list or None): A list of tuples representing the rows of data retrieved from the database.
        None: If there is an error in connection or execution, or if the table is empty
        """
        conn, cur = self.connect()

        if conn is None or cur is None:
            return None
        
        # Convert the list of columns into a comma-separated string
        columns_str = ', '.join(columns)
        
        try:
            if condition is None:
                query = f"SELECT {columns_str} FROM {table}"
            else:
                query = f"SELECT {columns_str} FROM {table} WHERE {condition}"

            cur.execute(query)
            data = cur.fetchall()
        except Exception as e:
            print("Error: Invalid data get\n", e)
            return None

        conn.commit()
        cur.close()
        conn.close()

        # If the table is empty, return "No data found"
        if len(data) == 0:
            return None

        return data

    def update_data(self, table: str, data: dict, condition=None) -> bool:
        """
        This method is used to update data in a specific table in the database.

        Parameters:
        table (str): The name of the table where the data will be updated.
        data (dict): A dictionary where the key is the column name and the value is the new data to be updated.
        condition (str, optional): The condition for the data update. Defaults to None.

        Returns:
        bool: True if the data was successfully updated, False otherwise.
        """
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return False

        values = []
        for key in data:
            # Format the values correctly for PostgreSQL
            values.append(f"{key} = '{data[key]}'")
        
        try:
            values_str = ', '.join(values)

            if condition is None:
                query = f"UPDATE {table} SET {values_str}"
            else:
                query = f"UPDATE {table} SET {values_str} WHERE {condition}"

            cur.execute(query)
        except Exception as e:
            print("Error: Invalid data update\n", e)
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True

    def delete_data(self, table: str, condition: str) -> bool:
        """
        This method is used to delete data from a specific table in the database.

        Parameters:
        table (str): The name of the table where the data will be deleted.
        condition (str): The condition for the data deletion.

        Returns:
        bool: True if the data was successfully deleted, False otherwise.
        """
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return False

        try:
            query = f"DELETE FROM {table} WHERE {condition}"
            cur.execute(query)
        except Exception as e:
            print("Error: Invalid data delete\n", e)
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True

    def create_table(self, table: str, columns: list, data_types: list) -> bool:
        """
        This method is used to create a table in the database.

        Parameters:
        table (str): The name of the table to be created.
        columns (list): A list of column names for the table.
        data_types (list): A list of data types for the columns.

        Returns:
        bool: True if the table was successfully created, False otherwise.
        """
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return False

        # Pair each column with its data type
        columns_with_types = ', '.join(f'{column} {data_type}' for column, data_type in zip(columns, data_types))
        
        try:
            query = f"CREATE TABLE IF NOT EXISTS {table} ({columns_with_types})"
            cur.execute(query)
        except Exception as e:
            print("Error: Invalid table creation\n", e)
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True

    def drop_table(self, table: str) -> bool:
        """
        This method is used to drop a table from the database.

        Parameters:
        table (str): The name of the table to be dropped.

        Returns:
        bool: True if the table was successfully dropped, False otherwise.
        """
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return False

        try:
            query = f"DROP TABLE IF EXISTS {table}"
            cur.execute(query)
        except Exception as e:
            print("Error: Invalid table drop\n", e)
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True

    def generate_random_data(self, table: str, columns: list, data_types: list, parameters: list, rows_number: int, text_len=1) -> bool:
        """
        This method is used to generate random data and insert it into a specific table in the database.

        Parameters:
        table (str): The name of the table where the data will be inserted.
        columns (list): A list of column names where the data will be inserted.
        data_types (list): A list of data types(in str) corresponding to the columns. Possible values:
            - int
            - text
            - date
            - time
            - timestamp
            - bool
            - fk_int
            
        parameters (list): A list of tuples, each containing a pair of parameters for the random data.
        rows_number (int): The number of rows of data to be generated and inserted.
        text_len (int, optional): The length of the text to be generated. Ignored if data_type is not text.

        Returns:
        bool: True if the data was successfully generated and inserted, False otherwise.
        """

        def handle_int(min_value: int, max_value: int) -> str:
            return f''' trunc(random() * ({max_value} - {min_value} + 1) + {min_value})::integer,'''
        
        def handle_text(min_value: int, max_value: int, text_len: int) -> str:
            random_selection = []
            for _ in range(text_len):
                random_selection.append(f"chr(trunc(random() * ({max_value} - {min_value} + 1) + {min_value})::int)")
            random_selection = " || ".join(random_selection)

            return f" {random_selection},"
        
        def handle_date(min_value: str, max_value: str) -> str:
            return f" (TIMESTAMP '{min_value}' + (random() * (TIMESTAMP '{max_value}' - TIMESTAMP '{min_value}'))::interval)::date,"
        
        def handle_time(min_value: str, max_value: str) -> str:
            return f" (random() * ('{max_value}'::time - '{min_value}'::time) + '{min_value}'::time)::time,"
        
        def handle_timestamp(min_value: str, max_value: str) -> str:
            return f" (date_trunc('second', TIMESTAMP '{min_value}' + (random() * (TIMESTAMP '{max_value}' - TIMESTAMP '{min_value}'))::interval))::timestamp,"
        
        def handle_bool() -> str:
            return f" (random() < 0.5)::bool,"
        
        def handle_foreign_key(parent_table: str, parent_column: str) -> str:
            return f'''
            (SELECT
                {parent_column}
            FROM
                {parent_table}
            ORDER BY
                random()
            LIMIT
                1),'''

        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return False
        
        try:
            columns = ', '.join(columns)
            query = f"INSERT INTO {table} ({columns}) SELECT"

            for parameter, data_type in zip(parameters, data_types):
                if data_type == 'fk_int':
                    parent_table, parent_column = parameter
                    query += handle_foreign_key(parent_table, parent_column)
                    
                elif data_type == 'int':
                    min_value, max_value = parameter
                    min_value = int(min_value)
                    max_value = int(max_value)
                    query += handle_int(min_value, max_value)
                    
                elif data_type == 'text':
                    min_value, max_value = parameter
                    min_value = int(min_value)
                    max_value = int(max_value)
                    query += handle_text(min_value, max_value, text_len)
                    
                elif data_type == 'date':
                    min_value, max_value = parameter
                    query += handle_date(min_value, max_value)
                    
                elif data_type == 'time':
                    min_value, max_value = parameter
                    query += handle_time(min_value, max_value)
                    
                elif data_type == 'timestamp':
                    min_value, max_value = parameter
                    min_value = ' '.join(min_value.split('/'))
                    max_value = ' '.join(max_value.split('/'))
                    query += handle_timestamp(min_value, max_value)
                    
                elif data_type == 'bool':
                    query += handle_bool()
                    
                else:
                    print(f"Error: Unsupported data type '{data_type}'")
                    return False

            query = query.rstrip(',') + f" FROM generate_series(1, {rows_number})"

            cur.execute(query)
        except Exception as e:
            print("Error: Invalid random data generation\n", e)
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True

    def pay_systems_total_income(self, left: int, right: int) -> Union[List[Tuple], None]:
        """
        This method is used to retrieve the total income of each pay system in the database.
        
        Parameters:
        left (int): The left bound of the sum of the orders.
        right (int): The right bound of the sum of the orders.
        
        Returns:
        data (list or None): A list of tuples representing the rows of data retrieved from the database.
        If there is an error in connection or execution, it returns None.
        """
        
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return None
        
        try:
            query = f'''
            SELECT
                tbl_pay_system.id,
                tbl_pay_system.name,
                COUNT(*) AS Count,
                SUM(tbl_order.sum) AS total
            FROM
                tbl_order
                INNER JOIN tbl_pay_system ON tbl_order.pay_system_id = tbl_pay_system.id
            WHERE
                sum BETWEEN {left} AND {right}
            GROUP BY
                tbl_pay_system.id,
                tbl_pay_system.name;
            '''
            cur.execute(query)
            data = cur.fetchall()
        except Exception as e:
            print("Error: Invalid random data generation\n", e)
            return None

        conn.commit()
        cur.close()
        conn.close()

        return data
    
    def company_orders_thru_period(self, left: str, right: str) -> Union[List[Tuple], None]:
        """
        This method is used to retrieve the number of orders placed by each company in the database.
        
        Parameters:
        left (str): The left bound of the period.
        right (str): The right bound of the period.
        
        Returns:
        data (list or None): A list of tuples representing the rows of data retrieved from the database.
        If there is an error in connection or execution, it returns None.
        """
        
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return None
        
        try:
            query = f'''
            SELECT
                tbl_company.id,
                tbl_company.name,
                COUNT(*) AS Count
            FROM
                tbl_order
                INNER JOIN tbl_company ON tbl_order.company_id = tbl_company.id
            WHERE
                tbl_order.date BETWEEN '{left}' AND '{right}'
            GROUP BY
                tbl_company.id,
                tbl_company.name;
            '''
            cur.execute(query)
            data = cur.fetchall()
        except Exception as e:
            print("Error: Invalid random data generation\n", e)
            return None

        conn.commit()
        cur.close()
        conn.close()

        return data
    
    def top_5_orders_total_price(self, company: str) -> Union[List[Tuple], None]:
        """
        This method is used to retrieve the top 5 orders with the highest total price for a specific company.
        
        Parameters:
        company (str): The name of the company.
        
        Returns:
        data (list or None): A list of tuples representing the rows of data retrieved from the database.
        If there is an error in connection or execution, it returns None.
        """
        
        conn, cur = self.connect()
        
        if conn is None or cur is None:
            return None
        
        try:
            query = f'''
            SELECT
                tbl_order.id,
                tbl_order.sum
            FROM
                tbl_order
                INNER JOIN tbl_company ON tbl_order.company_id = tbl_company.id
            WHERE
                tbl_company.name = '{company}'
            ORDER BY
                tbl_order.sum DESC
            LIMIT
                5;
            '''
            cur.execute(query)
            data = cur.fetchall()
        except Exception as e:
            print("Error: Invalid random data generation\n", e)
            return None

        conn.commit()
        cur.close()
        conn.close()

        return data
