from controller import Controller

DB_NAME = "CustomerCompaniesManagementSystem"
USER = "postgres"
HOST = "localhost"
PASSWORD = "1111"

if __name__ == "__main__":
    controller = Controller(DB_NAME, USER, PASSWORD, HOST)
    controller.run()
    