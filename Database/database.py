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
    
    def insert_record(self, table_name, data, topic = None):
        """
        Inserts a record into the specified table.
        data should be a dictionary where keys are column names and values are the data to be inserted.
        """
        
        if topic == "iot1/teaching_factory_fast/scale/final_weight":
            columns = ', '.join(data.keys())
            placeholders = ', '.join('?' * len(data))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
        elif topic == "iot1/teaching_factory_fast/temperature":
            columns = ', '.join(data.keys() + str(i))
            placeholders = ', '.join('?' * len(data))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            if i == 3:
                i = 1
            else:
                i += 1

        elif topic == "iot1/teaching_factory_fast/vibration":
            columns = ', '.join(data.keys())
            placeholders = ', '.join('?' * len(data))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        elif topic == "iot1/teaching_factory_fast/scale/is_cracked":
            columns = ', '.join(data.keys())
            placeholders = ', '.join('?' * len(data))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
        self.cursor.execute(insert_sql, tuple(data.values()))
        self.conn.commit()


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
    db = Database('teaching_factory.db')
    db.create_table('Bottles', {'id': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN'})
    db.create_table('Dispenser', {'color': 'TEXT', 'time_stamp': 'INTEGER PRIMARY KEY', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT', 'temp1': 'FLOAT', 'temp2': 'FLOAT', 'temp3': 'FLOAT'})
    db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})
    #db.insert_record('users', {'name': 'Alice', 'age': 30})
    #db.insert_record('users', {'name': 'Bob', 'age': 25})
    #records = db.fetch_records('users')
    #print(records)
    #db.delete_records('users', 'age < 30')
    #records = db.fetch_records('users')
    #print(records)
    db.close()