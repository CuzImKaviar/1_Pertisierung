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

    def insert_record(self, insert_sql, values):
        """
        Inserts a record into the specified table.
        data should be a dictionary where keys are column names and values are the values to insert.
        """
        self.cursor.execute(insert_sql, tuple(values))
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

# test von database.py
if __name__ == "__main__":
    db = Database('teaching_factory_test.db')
    db.create_table('Bottles', {'bottle': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN', 'time': 'INTEGER'})
    db.create_table('Dispenser_red', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Dispenser_blue', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Dispenser_green', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Temperature', {'time stamp': 'INTEGER PRIMARY KEY', 'temperature_C1': 'FLOAT', 'temperature_C2': 'FLOAT', 'temperature_C3': 'FLOAT'})
    db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})
    db.close()