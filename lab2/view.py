from tabulate import tabulate

class View:
    def show_message(self, message):
        print(message)
        
    def show_data(self, data, columns):
        print(tabulate(data, headers=columns, tablefmt="psql"))
        
    def get_insert_input(self):
        table = input("Enter table name: ")
        
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        data = input("Enter data separated by space: ")
        data = data.split()
        
        return table, columns, data
    
    def get_view_input(self):
        table = input("Enter table name: ")
        
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        condition = input("Enter condition in postgres SQL (... WHERE [condition]). If not applicable leave empty: ")
        if condition == "":
            condition = None
        return table, columns, condition
    
    def get_update_input(self):
        table = input("Enter table name: ")
        
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        data = input("Enter data separated by space: ")
        data = data.split()
        
        data = dict(zip(columns, data))
        
        condition = input("Enter condition in postgres SQL (... WHERE [condition]). If not applicable leave empty: ")
        if condition == "":
            condition = None
        return table, data, condition
    
    def get_delete_input(self):
        table = input("Enter table name: ")
        condition = input("Enter condition in postgres SQL (... WHERE [condition]). Can not be empty: ")
        if condition == "":
            raise ValueError("Condition can not be empty!")
        return table, condition
    
    def get_create_input(self):
        table = input("Enter table name: ")
        
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        data_types = input("Enter data types separated by space: ")
        data_types = data_types.split()
        
        return table, columns, data_types
    
    def get_drop_input(self):
        table = input("Enter table name: ")
        return table
    
    def get_generate_random_input(self):
        table = input("Enter table name: ")
        
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        data_types = input("Enter data types separated by space: ")
        data_types = data_types.split()
        
        parameters = input("Enter parameters separated by space: ")
        parameters = parameters.split()
        parameters = [tuple(parameter.split(",")) for parameter in parameters]
        
        rows_number = input("Enter number of rows: ")
        try:
            rows_number = int(rows_number)
        except ValueError:
            raise ValueError("Number of rows must be integer!")
            
        
        text_len = input("Enter length of text columns: ")
        text_len = int(text_len if text_len != "" else 0)
        
        return table, columns, data_types, parameters, rows_number, text_len
    
    def get_find_input(self):
        table = input("Enter table name: ")
        
        column = input("Enter column name: ")
        
        condition = ""
        t = input("Enter search type (number, string, boolean, date): ")
        if t == "number":
            left = input("Enter left bound: ")
            right = input("Enter right bound: ")
            condition = f"{column} BETWEEN {left} AND {right}"
        elif t == "string":
            string = input("Enter regex string: ")
            condition = f"{column} LIKE '%{string}%'"
        elif t == "boolean":
            boolean = input("Enter boolean value (True, False): ")
            condition = f"{column} = {boolean}"
        elif t == "date":
            left = input("Enter left bound (YYYY-MM-DD): ")
            right = input("Enter right bound (YYYY-MM-DD): ")
            condition = f"{column} BETWEEN '{left}' AND '{right}'"
        
        if condition == "":
            condition = None
        return table, column, condition
    
    def get_pay_systems_total_income_input(self):
        left = input("Enter left bound (starting number): ")
        right = input("Enter right bound (last number): ")
        return left, right
    
    def get_company_orders_thru_period_input(self):
        left = input("Enter left bound (starting date YYYY-MM-DD): ")
        right = input("Enter right bound (last date YYYY-MM-DD): ")
        return left, right
    
    def get_top_5_orders_total_price_input(self):
        company = input("Enter company name: ")
        return company
    