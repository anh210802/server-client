import socket
import threading
import logging
import mysql.connector
from mysql.connector import Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clients = {}
        self.lock = threading.Lock()
        self.connection = None
        self.cursor = None

    def start(self):
        try:
            self.server.bind((self.host, self.port))
            self.server.listen(5)
            self.start_sql()
            logging.info(f"Server listening on {self.host}:{self.port}")
            while True:
                client_socket, client_address = self.server.accept()
                logging.info(f"New client connection from {client_address}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, client_address))
                client_thread.start()
        except socket.error as e:
            logging.error(f"Can't start server: {e}")

    def start_sql(self):
        try:
            self.connection = mysql.connector.connect(
                host='localhost',
                user='root',
                password='123456',
                database='data_iot'
            )
            self.cursor = self.connection.cursor()
            self.create_tables()
            logging.info("SQL connection established")
        except Error as e:
            logging.error(f"Error establishing SQL connection: {e}")

    def create_tables(self):
        try:
            create_users_table_query = """
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                password VARCHAR(255) NOT NULL
            )
            """
            create_sensor_data_table_query = """
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                temperature FLOAT,
                humidity FLOAT,
                pm1 FLOAT,
                pm25 FLOAT,
                pm10 FLOAT,
                co_value FLOAT,
                max_value FLOAT,
                rate INT
            )
            """
            self.cursor.execute(create_users_table_query)
            self.cursor.execute(create_sensor_data_table_query)
            self.connection.commit()
            logging.info("Ensured that users and sensor_data tables exist")
        except Error as e:
            logging.error(f"Error creating tables: {e}")

    def handle_client(self, client_socket, client_address):
        try:
            while True:
                data_received = client_socket.recv(1024).decode()
                if not data_received:
                    break

                state, data_sent = data_received.split(",", 1)
                logging.info(f"Client from {client_address} sent state: {state}")

                if state == "login":
                    self.login_client(client_socket, data_sent)
                elif state == "get_sensor_data":
                    self.get_sensor_data(client_socket)

        except ConnectionResetError:
            logging.warning(f"Connection reset by {client_address}")
        except Exception as e:
            logging.error(f"Error handling client {client_address}: {e}")
        finally:
            logging.info(f"Connection closed by {client_address}")
            client_socket.close()

    def login_client(self, client_socket, data):
        logging.info(f"Login data: {data}")
        username, password = data.split(",")

        query = "SELECT * FROM users WHERE username = %s AND password = %s"
        self.lock.acquire()
        try:
            self.cursor.execute(query, (username, password))
            result = self.cursor.fetchone()
            if result:
                client_socket.send("Login successful".encode())
                logging.info("Login successful")
            else:
                client_socket.send("Login failed".encode())
                logging.info("Login failed")
        finally:
            self.lock.release()

    def get_sensor_data(self, client_socket):
        query = "SELECT * FROM sensor_data"
        self.lock.acquire()
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            if result:
                for row in result:
                    data = ",".join(map(str, row))
                    client_socket.send(f"{data}\n".encode())
                client_socket.send("END_OF_DATA".encode())
                logging.info("Sensor data sent to client")
            else:
                client_socket.send("No sensor data available".encode())
                logging.info("No sensor data available")
        except Error as e:
            logging.error(f"Error fetching sensor data: {e}")
        finally:
            self.lock.release()

if __name__ == "__main__":
    server = Server("localhost", 12345)
    server.start()
