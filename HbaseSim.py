import time
from datetime import datetime
import pandas as pd
import os
import random
import numpy as np
import ast
from io import StringIO
import json


class HbaseSimulator:
    def __init__(self) -> None:
        self.IP = "198.167.0.1"
        self.tables = {}
        self.table_names = self.get_tables()
    # -----------------------------helper functions-----------------------------

    # Function to get all the tables in the HbaseCollections folder
    def get_tables(self):
        directory = "./HbaseCollections"
        all_entries = os.listdir(directory)
        file_names = [entry for entry in all_entries if os.path.isfile(
            os.path.join(directory, entry))]
        file_names = [file_name.replace(".json", "")
                      for file_name in file_names]
        return file_names

    # Function to get the number of rows in a table
    def count_rows(self, table_name: str) -> int:
        with open(f'./HbaseCollections/{table_name}.json', 'r') as file:
            data = json.load(file)
            count = len(data)
        return count

    def check_string_in_file(self, search_string):
        with open('./disabledTables.txt', 'r') as file:
            for line in file:
                if search_string in line:
                    return True
        return False

    def table_exists(self, table_name: str) -> bool:
        if table_name in self.table_names:
            return True
        return False

    # -----------------------------hbase functions-----------------------------

    def get(self, command: str) -> bool:
        command = command.replace("get", "")
        commands = command.split(",")

        if len(commands) != 2:
            print(f"\n=> Hbase::get - Incorrect command format {command}\n")
            return False

        table_name = commands[0].replace(" ", "").replace("'", "")
        row_key = commands[1].replace(" ", "").replace("'", "")

        if not os.path.exists(f"./HbaseCollections/{table_name}.json"):
            print(f"\n=> Hbase::get - Table {table_name} does not exist.\n")
            return False

        with open(f"./HbaseCollections/{table_name}.json", 'r') as file:
            data = json.load(file)

        headers = data["headers"]
        rows = data["rows"]
        if row_key not in rows:
            print(f"\n=> Hbase::get - Row Key {row_key} not found in table {table_name}\n")
            return False

        row_data = rows[row_key]
        print(f"\n=> Hbase::get - Row Key: {row_key}\n")
        for header in headers:
            value = row_data[header]
            print(f"{header}: {value}")

        return True


    def scan(self, command: str):
        command = command.replace("scan", "").replace(' ', '').split(",")

        # Checking the command syntax
        if len(command) != 1:
            print(
                f"\nValue error on: {command}\nToo many arguments for scan function.\nUsage: scan '<table_name>'\n"
            )
            return False

        command = command[0].replace("'", "")
        # Checking if the table exists
        if command not in self.table_names:
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False

        # checking if the table is disabled
        if self.check_string_in_file(command):
            print(f"\n=> Hbase::Table - {command} is disabled.\n")
            return False

        with open(f'./HbaseCollections/{command}.json', 'r') as file:
            data = json.load(file)
        
        headers_dict = data['headers']
        rows_dict = data['rows']
        
        df = pd.DataFrame.from_dict(rows_dict, orient='index')
        
        print('row key ,',str(headers_dict).replace("[",'').replace("]",'').replace("'",''))
        for element in df:
            element
        
        return True

    def delete(self, command: str) -> bool:
        command = command.replace("delete", "")
        commands = command.split(",")

        if len(commands) != 2:
            print(f"\n=> Hbase::delete - Incorrect command format {command}\n")
            return False

        table_name = commands[0].replace(" ", "").replace("'", "")
        row_key = commands[1].replace(" ", "").replace("'", "")

        if not os.path.exists(f"./HbaseCollections/{table_name}.json"):
            print(f"\n=> Hbase::delete - Table {table_name} does not exist.\n")
            return False

        # Checking if the table is disabled
        if self.check_string_in_file(table_name):
            print(f"\n=> Hbase::delete - Table {table_name} is disabled.\n")
            return False

        # Reading the JSON file
        with open(f"./HbaseCollections/{table_name}.json", 'r') as file:
            data = json.load(file)

        # Checking if the row key exists
        if row_key not in data['rows']:
            print(f"\n=> Hbase::delete - Row key {row_key} not found in table {table_name}\n")
            return False

        # Deleting the row
        del data['rows'][row_key]

        # Updating the JSON file
        with open(f"./HbaseCollections/{table_name}.json", 'w') as file:
            json.dump(data, file)

        print(f"\n=> Hbase::delete - Row key {row_key} deleted from table {table_name}\n")
        return True

    def count(self, table_name: str, search_param: str = None) -> int:
        # Verificar si la tabla existe
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False

        with open(f'./HbaseCollections/{table_name}.json', 'r') as file:
            data = json.load(file)
            rows_dict = data['rows']

            # Contar el total de filas
            if search_param is None:
                count = len(rows_dict)
                print(f"\n=> Hbase::Table - {table_name} has {count} rows.\n")
                return count

            # Contar solo filas que coinciden con el parámetro de búsqueda
            else:
                row_count = 0
                for row in rows_dict.values():
                    if search_param in row.values():
                        row_count += 1
                print(
                    f"\n=> Hbase::Table - {table_name} has {row_count} rows that match the search parameter '{search_param}'.\n")
                return row_count

    def truncate(self, table_name: str) -> bool:
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False

        if self.check_string_in_file(table_name):
            print(f"\n=> Hbase::Table - {table_name} is disabled.\n")
            return False

        self.disable(f"disable '{table_name}'")

        # abrir el archivo de la tabla
        with open(f'./HbaseCollections/{table_name}.json', 'r') as file:
            writer = csv.writer(file)
            headers = next(writer)
            writer.writerow(headers)
        print(f"\n=> Hbase::Table - {table_name} truncated.\n")

        self.disable(f"enable '{table_name}'")
        return True

    def disable(self, command):
        # Setting the start time of the function
        start = time.time()
        # Removing the disable command from the command and splitting the command
        command = command.replace("disable", "").replace(' ', '').split(",")
        # Checking the command syntax
        if len(command) < 1:
            print(
                f"\nValue error on: {command}\nToo many arguments for disable fuction.\nUsage: disable '<table_name>'\n"
            )
            return False
        elif command[0][-1] != "'" or command[0][0] != "'":
            print(
                f"\nSyntax error on: {command[0]}\nCorrect use of single quotes is required.\nUsage: disable '<table_name>'\n"
            )
            return False
        # Getting the table name from the command
        command = command[0].replace("'", "")

        direc = './HbaseCollections'
        filename = f'{command}.json'
        file_path = os.path.join(direc, filename)

        # Checking if the table exists
        if os.path.exists(file_path):
            # Write the name of the table on the disabled tables txt file
            with open("./disabledTables.txt", 'a+') as file:
                file.seek(0)
                file_contents = file.read()
                if command not in file_contents:
                    file.write(command)
                    file.write('\n')
        else:
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False

        # Setting the end time of the function and printing the results
        end = time.time()
        # printing the results
        print(f'0 row(s) in {round(end-start,4)} seconds')
        print(f"\n=> Hbase::Table - {command} disabled")

        return True

    # Modifies a table
    def alter(self, command: str) -> bool:
        # Setting the start time of the function
        start_time = time.time()
        # Removing the alter command from the command and splitting the command
        command = command.replace("alter", "").replace(' ', '').split(",")
        if len(command) < 3:
            print(
                f"\nValue error on: {command}\nToo few arguments for alter fuction.\nUsage: alter '<table_name>', '<column_family_name>', '<column_family_action>'\n"
            )
            return False

        # Getting the meta from the command
        value = command[0].replace("'", "")
        cf = command[1].replace("'", "")
        action = command[2].replace("'", "")

        # Checking if the table exists
        if not os.path.exists(f"./HbaseCollections/{value}.json"):
            print(f"\n=> Hbase::Table - {value} does not exist.\n")
            return False

        # Updating the headers of the table
        with open(f"./HbaseCollections/{value}.json", "r+") as file:
            data = json.load(file)

            if action == "add":
                if cf not in data["headers"]:
                    data["headers"].append(cf)
                    for key in data["rows"]:
                        data["rows"][key][cf] = ""
                    file.seek(0)
                    json.dump(data, file, indent=4)
                    file.truncate()
                    print(f"\n=> Hbase::Table - Added {cf} to {value}.\n")
                    return True
                else:
                    print(f"\n=> Hbase::Table - {cf} already exists in {value}.\n")
                    return False

            elif action == "delete":
                if cf in data["headers"]:
                    data["headers"].remove(cf)
                    for key in data["rows"]:
                        del data["rows"][key][cf]
                    file.seek(0)
                    json.dump(data, file, indent=4)
                    file.truncate()
                    print(f"\n=> Hbase::Table - Deleted {cf} from {value}.\n")
                    return True
                else:
                    print(f"\n=> Hbase::Table - {cf} does not exist in {value}.\n")
                    return False

            else:
                print(
                    f"\nValue error on: {command}\nUnknown command for alter fuction.\nUsage: alter '<table_name>', '<column_family_name>', 'add/delete'\n"
                )
                return False
            

    def drop(self, table_name: str) -> bool:
        # Verificar si la tabla existe
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False

        # Eliminar el archivo JSON de la tabla
        try:
            os.remove(f"./HbaseCollections/{table_name}.json")
        except OSError:
            print(f"\n=> Hbase::Table - Error deleting {table_name}.json\n")
            return False

        # Remover el nombre de la tabla de la lista de tablas
        self.table_names.remove(table_name)
        print(f"\n=> Hbase::Table - {table_name} dropped.\n")
        return True

    def dropAll(self):
        for table_name in self.table_names:
            if os.path.exists(f"./HbaseCollections/{table_name}.json"):
                os.remove(f"./HbaseCollections/{table_name}.json")
            else:
                print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
        self.table_names = []
        print("\n=> Hbase::All tables dropped\n")
        return True

    # Describes a table
    def describe(self, command: str) -> bool:
        # Setting the start time of the function
        start_time = time.time()
        # Removing the describe command from the command and splitting the command
        command = command.replace("describe", "").replace(' ', '').replace("'", "")
        # Checking if the table exists
        if not os.path.exists(f"./HbaseCollections/{command}.json"):
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False

        # Getting the headers of the table
        with open(f"./HbaseCollections/{command}.json", "r") as file:
            data = json.load(file)
        headers = list(data.keys())
        # Printing the results
        print(f"\nTable {command}")
        print(f"{len(headers)} column(s)")
        for header in headers:
            print(header)
        # Setting the end time of the function
        end_time = time.time()
        print(f"\n=> Hbase::Table - {command} described in {round(end_time - start_time, 4)} seconds\n")
        return True

    def load_table(self, table: str):
        if table not in self.tables:
            if not self.table_exists(table):
                print(f"\n=> Hbase::Table - {table} does not exist.\n")
                return False
            else:
                with open(f"./HbaseCollections/{table}.json", "r") as f:
                    self.tables[table] = json.load(f)
        else:
            with open(f"./HbaseCollections/{table}.json", "r") as f:
                self.tables[table] = json.load(f)
        return True


    # Creates a table
    def create(self, command: str) -> bool:
        # Setting the start time of the function
        start_time = time.time()
        # Removing the create command from the command and splitting the command
        command = command.replace("create", "").replace(' ', '').split(",")
        if len(command) < 2:
            print(
                f"\nValue error on: {command}\nToo few arguments for create fuction.\nUsage: create '<table_name>', '<column_family_name>'\n"
            )
            return False
        # Getting the meta from the command
        value = command[0].replace("'", "")
        timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        rowKey = len(self.table_names) + 1

        # checking if the table already exists
        if value in self.table_names:
            print(f"\n=> Hbase::Table - {value} already exists.\n")
            return False

        # Checking the command syntax
        for spec in command:
            if spec[0] != "'" or spec[-1] != "'":
                print(
                    f"\nSyntax error on: {spec}\nCorrect use of single quotes is required.\nUsage: create '<table_name>', '<column_family_name>'\n"
                )
                return False

        command = [spec.replace("'", "") for spec in command]
        

        # setting the meta data
        meta_data = {
            rowKey:{
                'Timestamp': timestamp,
                'Value': value
            },   
        }
        # removing the table name from the list
        command.remove(value) 
        
        if value not in self.table_names:
            # Define the directory and file names
            dir_path = "./HbaseCollections/"
            file_name = "TABLE.json"
            # Check if the directory is empty or if the file does not exist
            if not os.path.exists(dir_path) or not os.listdir(dir_path):
                # The directory is empty or the file does not exist, so create a new JSON file
                with open(f"{dir_path}{file_name}", 'w') as json_file:
                    json.dump(meta_data, json_file)
            else:
                # The directory is not empty, so load the existing JSON file into a dictionary
                with open(f"{dir_path}{file_name}", 'r') as json_file:
                    existing_data = json.load(json_file)
                
                # Add the new key-value pair to the dictionary
                existing_data.update(meta_data)
                
                # Write the updated dictionary back to the JSON file
                with open(f"{dir_path}{file_name}", 'w') as json_file:
                    json.dump(existing_data, json_file)
                    
            # Creating the json table
            headers = command
            rows = {}
            table = {'headers': headers, 'rows': rows}
            # Adding the headers to de HFile
            with open(f"./HbaseCollections/{value}.json", 'w') as json_file:
                json.dump(table,json_file)
                
        else:
            print(f"\n=> Hbase::Table - {value} already exists.\n")
            return False

        # Adding the table to the tables dictionary
        self.table_names.append(value)
        # Setting the end time of the function and printing the results
        end_time = time.time()
        print(f'0 row(s) in {round(end_time - start_time,4)} seconds')
        print(f"\n=> Hbase::Table - {value} created")
        return True

    # Lists all the tables
    def list_(self):

        files = os.listdir("./HbaseCollections")
        for file in files:
            if os.path.isfile(os.path.join("./HbaseCollections", file)):
                print(file.replace(".json", ""))

    def delete_all(self, command: str) -> bool:
        table_name = command.split(" ")[1].replace("'", "")
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False
        # Deshabilitar la tabla
        self.disable(f"disable '{table_name}'")
        # Cargar datos de la tabla desde el archivo JSON
        with open(f"./HbaseCollections/{table_name}.json", "r") as f:
            data = json.load(f)
        total_rows = len(data)
        # Borrar el archivo JSON de la tabla
        os.remove(f"./HbaseCollections/{table_name}.json")
        # Volver a habilitar la tabla
        self.disable(f"enable '{table_name}'")
        # Imprimir filas eliminadas
        print(f"\n=> Hbase::Table - {table_name} deleted {total_rows} rows.\n")
        return True
    
    def insert_many(self, command: str) -> bool:
        # Parse the command to extract the table name and the data
        parts = command.split("values")[0].split()
        table_name = parts[2].replace("'", "")
        column_names = [col.replace("'", "") for col in parts[3:-1]]
        data = json.loads(command.split("values")[1])

        # Load the table data from disk if it exists, otherwise create an empty dataframe
        if table_name not in self.tables:
            if not self.table_exists(table_name):
                print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
                return False
            self.tables[table_name] = pd.DataFrame(columns=["id"] + column_names)

        table = self.tables[table_name]

        # Append each row of data to the table
        for row in data:
            # Extract the ID from the row
            id = row[0]

            # Check if the row already exists in the table
            if id in table["id"].values:
                print(f"\n=> Hbase::Table - {table_name} - Row with ID '{id}' already exists, skipping insertion.\n")
                continue

            # Create a new row with the ID and the rest of the data
            new_row = {"id": id}
            for i, val in enumerate(row[1:]):
                new_row[column_names[i]] = val

            # Append the new row to the table
            table = table.append(new_row, ignore_index=True)

        # Save the updated table data to disk
        table.to_json(f"./HbaseCollections/{table_name}.json", orient="records")

        print(f"\n=> Hbase::Table - {table_name} - Inserted {len(data)} rows.\n")
        return True


    def update_many(self, command: str) -> bool:
        command = command.replace("update_many", "").replace(' ', '').split(",")
        command = [spec.replace("'", "") for spec in command]

        # Extract the table name and check if it exists
        table = command.pop(0)
        self.load_table(table)

        if table not in self.tables:
            return False

        df = pd.read_csv(f'./HbaseCollections/{table}.csv')

        column_subcol = command.pop(0).split(":")
        subcol = column_subcol[1]

        updates = {}
        for i in range(0, len(command), 2):
            updates[command[i]] = command[i+1]

        for id, value in updates.items():
            # Check if the id is in the table, if not, create a new row, if it is, update the row
            if id in df['id'].values:
                row = df.loc[df['id'] == id, column_subcol[0]].iloc[0]
                sub_col = pd.read_csv(StringIO(str(row)), index_col=0)
                sub_col.loc[subcol, 0] = str(value)
                df.loc[df['id'] == id, column_subcol[0]] = sub_col.to_csv(header=False, index=False)
            else:
                new_row = {'id': id, f'{column_subcol[0]}': pd.DataFrame({f'{subcol}': [str(value)]})}
                new_row_df = pd.DataFrame.from_dict(new_row, orient='index').T
                df = pd.concat([df, new_row_df], ignore_index=True)
                
        df.to_csv(f'./HbaseCollections/{table}.csv', index=False)
        return True


    def mainHBase(self):
        is_enabled = True
        counter = 0
        initial = input("\n[cloudera@quickstart ~]$ ")
        # time.sleep(2)
        # Start the Hbase shell
        if initial == "hbase shell":
            print(
                f"{datetime.today().strftime('%Y-%m-%d')} \nINFO [main] Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available\n Hbase shell enter 'help<RETURN>' for list of supported commands. Type 'exit<RETURN>' to leave the HBase Shell\n Version 1.4.13, rUnknown\n")

            command = ""
            while is_enabled and command != "exit<RETURN>" or command != "exit":
                # User enters any command of the Hbase shell
                command = input(f"hbase(main):00{counter}:0>")
                counter += 1

                # Status commmand
                if command == 'status':
                    print(
                        '1 active master, 0 backup masters, 1 servers, 0 dead, 1.0000 average load')

                # Version command
                elif command == 'version':
                    print(
                        f'1.4.13, rUnknown, {datetime.today().strftime("%Y-%m-%d")}')

                # TODO table help command
                elif command == 'table_help':
                    pass
                # whoami command
                elif command == "whoami":
                    print("cloudera (auth:SIMPLE)\n     groups: cloudera, default")

                # Create table command
                elif 'create' == command.split(" ")[0]:
                    # TODO Implement create table function
                    self.create(command)

                elif 'insert_many' == command.split(" ")[0]:
                    # TODO Implement create table function
                    self.insert_many(command)

                elif 'update_many' == command.split(" ")[0]:
                    # TODO Implement create table function
                    self.update_many(command)

                # List tables command
                elif command == 'list':
                    self.list_()

                # Disable table command
                elif 'disable' == command.split(" ")[0]:
                    self.disable(command)

                elif 'scan' == command.split(" ")[0]:
                    self.scan(command)

                elif 'count' == command.split(" ")[0]:
                    # conseguir el nombre de la tabla y el nombre de la columna
                    args = command.split(" ")
                    if len(args) < 2 or len(args) > 3:
                        print(
                            "Usage: count '<table_name>' [, '<search_string>']")
                    elif len(args) == 2:
                        table_name = args[1].replace("'", "")
                        self.count(table_name)
                    else:
                        table_name = args[1].replace("'", "")
                        search_string = args[2].replace("'", "")
                        self.count(table_name, search_string)

                # Drop table command
                elif 'drop' == command.split(" ")[0]:
                    table_name = command.split(" ")[1].replace("'", "")
                    hbase.drop(table_name)

                elif 'drop_all' == command.split(" ")[0]:
                    hbase.drop_all()

                elif 'delete' == command.split(" ")[0]:
                    self.delete(command)

                elif 'deleteall' == command.split(" ")[0]:
                    self.delete_all(command)

                elif 'truncate' == command.split(" ")[0]:
                    table_name = command.split(" ")[1].replace("'", "")
                    self.truncate(table_name)

                elif 'put' == command.split(" ")[0]:
                    self.put(command)

                elif 'get' == command.split(" ")[0]:
                    self.get(command)

                elif command != '':
                    print(f"ERROR: Unknown command '{command}'")
        elif initial != '':
            print(f"ERROR: Unknown command '{initial}'")

    

    def put(self, command: str) -> bool:
        command = command.replace("put", "").replace(' ', '').split(",")
        command = [spec.replace("'", "") for spec in command]

        # Extract the table name and check if it exists
        table = command.pop(0)
        self.load_table(table)

        if table not in self.tables:
            return False

        df = pd.read_csv(f'./HbaseCollections/{table}.csv')

        id = command[0]
        column_subcol = command[1].split(":")
        value = command[2]
        
        print(id)
        print(column_subcol)
        print(value)
        print("\n")
        
        ids = [str(id) for id in df['id'].values]

        # Check if the id is in the table, if not, create a new row, if it is, update the row
        if id not in ids:
            print("id not in df")    
            new_row = {'id':id,f'{column_subcol[0]}':pd.DataFrame({f'{column_subcol[1]}':[str(value)]})}
            new_row_df = pd.DataFrame.from_dict(new_row, orient='index').T
            df = pd.concat([df, new_row_df], ignore_index=True)
            df.to_csv(f'./HbaseCollections/{table}.csv', index=False)
        else:
            print("id in df")
            # Read the CSV file into a dataframe
            df = pd.read_csv(f'./HbaseCollections/{table}.csv', dtype={'id': str})

            # Extract the row with ID = 100
            row = df[df['id'] == id]

            # Extract the nested dataframe from the 'personal_data' column
            row_str = str(row[column_subcol[0]].iloc[0]).strip()
            sub_col = pd.read_csv(StringIO(row_str))

            # If the sub col exists uptade value, else create it
            if column_subcol[1] in sub_col.columns:
                print(column_subcol[1],'exists')
                print('Value updated\n')
                sub_col.loc[0, column_subcol[1]] = str(value)
                df.loc[df['id'] == id, column_subcol[0]] = sub_col.to_csv(index=False)
                df.to_csv(f'./HbaseCollections/{table}.csv', index=False)
                
            else:
                print(column_subcol[1],'does not exist')
                new_val = pd.DataFrame.from_dict({f'{column_subcol[1]}':[str(value)]})
                print(new_val)
                    
            
            
        return True
    
def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')


hbase = HbaseSimulator()
clear_screen()
# hbase.mainHBase()

# hbase.create("create 'empleado', 'personal_data', 'empresa'")

hbase.scan("scan 'empleado'")