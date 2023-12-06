from model import Model
from view import View


class Controller:
    def __init__(self, db_name, user, password, host):
        self.model = Model(db_name, user, password, host)
        self.view = View()

    def run(self):
        while True:
            self.show_tables()
            choice = self.show_menu()
            if choice == "1":
                self.insert_data()
            elif choice == "2":
                self.view_data()
            elif choice == "3":
                self.update_data()
            elif choice == "4":
                self.delete_data()
            elif choice == "5":
                self.create_table()
            elif choice == "6":
                self.drop_table()
            elif choice == "7":
                self.generate_random_data()
            elif choice == "8":
                self.find_data()
            elif choice == "9":
                a = self.show_algorithms()
                if a == "1":
                    self.pay_systems_total_income()
                elif a == "2":
                    self.company_orders_thru_period()
                elif a == "3":
                    self.top_5_orders_total_price()
                elif a == "0":
                    continue
            elif choice == "0":
                break
            else:
                self.view.show_message("Invalid choice!")
                
    def show_tables(self):
        tables = self.model.get_tables()
        tables = [table[0] for table in tables]
        self.view.show_message(f"\nAvailable tables: {tables if tables is not None else 'None'}")

    def show_menu(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Insert Data")
        self.view.show_message("2. View Data")
        self.view.show_message("3. Update Data")
        self.view.show_message("4. Delete Data")
        self.view.show_message("5. Create Table")
        self.view.show_message("6. Drop Table")
        self.view.show_message("7. Generate Random Data")
        self.view.show_message("8. Find Data")
        self.view.show_message("9. Algorithms")
        self.view.show_message("0. Quit")
        return input("Enter your choice: ")
    
    def show_algorithms(self):
        self.view.show_message("\nAlgorithms:")
        self.view.show_message("1. Pay Systems' Total Income")
        self.view.show_message("2. Company's Orders' thru Period")
        self.view.show_message("3. Top 5 Orders' Total Price")
        self.view.show_message("0. Quit")
        return input("Enter your choice: ")
        
    def insert_data(self):
        table, columns, data = self.view.get_insert_input()
        if self.model.insert_data(table, columns, data):
            self.view.show_message("Data inserted successfully!")
        else:
            self.view.show_message("Data insertion failed!")
        
    def view_data(self):
        table, columns, condition = self.view.get_view_input()
        data = self.model.get_data(table, columns, condition)
        if data is not None:
            self.view.show_data(data, columns)
        else:
            self.view.show_message("Data retrieval failed!")

    def update_data(self):
        table, data, condition = self.view.get_update_input()
        if self.model.update_data(table, data, condition):
            self.view.show_message("Data updated successfully!")
        else:
            self.view.show_message("Data update failed!")

    def delete_data(self):
        table, condition = self.view.get_delete_input()
        if self.model.delete_data(table, condition):
            self.view.show_message("Data deleted successfully!")
        else:
            self.view.show_message("Data deletion failed!")
            
    def create_table(self):
        table, columns, data_types = self.view.get_create_input()
        if self.model.create_table(table, columns, data_types):
            self.view.show_message("Table created successfully!")
        else:
            self.view.show_message("Table creation failed!")
            
    def drop_table(self):
        table = self.view.get_drop_input()
        if self.model.drop_table(table):
            self.view.show_message("Table dropped successfully!")
        else:
            self.view.show_message("Table drop failed!")
            
    def generate_random_data(self):
        table, columns, data_types, parameters, rows_number, text_len = self.view.get_generate_random_input()
        if self.model.generate_random_data(table, columns, data_types, parameters, rows_number, text_len):
            self.view.show_message("Random data generated successfully!")
        else:
            self.view.show_message("Random data generation failed!")
            
    def find_data(self):
        table, column, condition = self.view.get_find_input()
        data = self.model.get_data(table, [column], condition)
        if data is not None:
            self.view.show_data(data, [column])
        else:
            self.view.show_message("Data retrieval failed!")
            
    def pay_systems_total_income(self):
        left, right = self.view.get_pay_systems_total_income_input()
        data = self.model.pay_systems_total_income(left, right)
        if data is not None:
            self.view.show_data(data, ["id", "name", "count", "total_income"])
        else:
            self.view.show_message("Data retrieval failed!")
            
    def company_orders_thru_period(self):
        left, right = self.view.get_company_orders_thru_period_input()
        data = self.model.company_orders_thru_period(left, right)
        if data is not None:
            self.view.show_data(data, ["id", "company", "orders"])
        else:
            self.view.show_message("Data retrieval failed!")
            
    def top_5_orders_total_price(self):
        company = self.view.get_top_5_orders_total_price_input()
        data = self.model.top_5_orders_total_price(company)
        if data is not None:
            self.view.show_data(data, ["order_id", "total_price"])
        else:
            self.view.show_message("Data retrieval failed!")
