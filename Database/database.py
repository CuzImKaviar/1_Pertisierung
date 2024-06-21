import sqlite3

class Database:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
    
    def create_table(self, table_name, columns):
        """
        Creates a table with the given name and columns.
        columns should be a dictionary where keys are column names and values are data types.
        """
        columns_def = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def});"
        self.cursor.execute(create_table_sql)
        self.conn.commit()
    
    # def handle_final_weight(self, data, table_name):
    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join('?' * len(data))
    #     insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    #     return insert_sql
    
    # def handle_temperature(self, data, table_name, i):
    #     columns = ', '.join(list(data.keys()) + [str(i)])
    #     placeholders = ', '.join('?' * len(data))
    #     insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    #     if i == 3:
    #         i = 1
    #     else:
    #         i += 1
    #     return insert_sql, i
    
    # def handle_vibration(self, data, table_name):
    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join('?' * len(data))
    #     insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    #     return insert_sql
    
    # def handle_is_cracked(self, data, table_name):
    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join('?' * len(data))
    #     insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    #     return insert_sql
    
    # def handle_dispenser(self, data, table_name):
    #     data.pop('dispenser')
    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join('?' * len(data))
    #     insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    #     return insert_sql
    
    # def handle_default(self, data, table_name):
    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join('?' * len(data))
    #     insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    #     return insert_sql
    
    # def insert_record(self, topic, data, table_name, i):
    #     switcher = {
    #         "iot1/teaching_factory_fast/scale/final_weight": self.handle_final_weight,
    #         "iot1/teaching_factory_fast/temperature": self.handle_temperature,
    #         "iot1/teaching_factory_fast/vibration": self.handle_vibration,
    #         "iot1/teaching_factory_fast/scale/is_cracked": self.handle_is_cracked,
    #         "iot1/teaching_factory_fast/dispenser_red": self.handle_dispenser,
    #         "iot1/teaching_factory_fast/dispenser_blue": self.handle_dispenser,
    #         "iot1/teaching_factory_fast/dispenser_green": self.handle_dispenser
    #     }
    #     handler = switcher.get(topic, self.handle_default)
    #     if handler == self.handle_temperature:
    #         insert_sql, i = handler(data, table_name, i)
    #     else:
    #         insert_sql = handler(data, table_name)
    #     self.cursor.execute(insert_sql, tuple(data.values()))
    #     self.conn.commit()

    def insert_record(self, insert_sql, data):
        """
        Inserts a record into the specified table.
        data should be a dictionary where keys are column names and values are the values to insert.
        """
        self.cursor.execute(insert_sql, tuple(data.values()))
        self.conn.commit()

    def exists(self, table_name, condition):
        """
        Checks if a record exists in the specified table based on the condition.
        condition is an SQL WHERE clause to filter records.
        """
        fetch_sql = f"SELECT * FROM {table_name} WHERE {condition}"
        self.cursor.execute(fetch_sql)
        return len(self.cursor.fetchall()) > 0

    def fetch_records(self, table_name, condition=None):
        """
        Fetches records from the specified table.
        condition is an optional SQL WHERE clause to filter records.
        """
        fetch_sql = f"SELECT * FROM {table_name}"
        if condition:
            fetch_sql += f" WHERE {condition}"
        self.cursor.execute(fetch_sql)
        return self.cursor.fetchall()

    def delete_records(self, table_name, condition):
        """
        Deletes records from the specified table based on the condition.
        condition is an SQL WHERE clause to filter records for deletion.
        """
        delete_sql = f"DELETE FROM {table_name} WHERE {condition}"
        self.cursor.execute(delete_sql)
        self.conn.commit()

    def close(self):
        """
        Closes the database connection.
        """
        self.conn.close()

# Example usage:
if __name__ == "__main__":
    db = Database('teaching_factory_test.db')
    db.create_table('Bottles', {'bottle': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN', 'time': 'INTEGER'})
    db.create_table('Dispenser_red', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Dispenser_blue', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Dispenser_green', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Temperature', {'time stamp': 'INTEGER PRIMARY KEY', 'temperature_C1': 'FLOAT', 'temperature_C2': 'FLOAT', 'temperature_C3': 'FLOAT'})
    db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})
    #db.insert_record('users', {'name': 'Alice', 'age': 30})
    #db.insert_record('users', {'name': 'Bob', 'age': 25})
    #records = db.fetch_records('users')
    #print(records)
    #db.delete_records('users', 'age < 30')
    #records = db.fetch_records('users')
    #print(records)
    db.close()