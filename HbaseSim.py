import time
from datetime import datetime
import pandas as pd
import os
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


    def format_nested_json(self, json_data):
        # Define a function to format each row of data
        def format_row(row_id, data):
            row_key = row_id
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            columns = []
            values = []
            for header in json_data['headers']:
                if header in data[row_id]:
                    if isinstance(data[row_id][header], dict):
                        sub_columns = []
                        for sub_col in data[row_id][header]:
                            sub_columns.append(sub_col)
                        columns.append(header + ':' + ','.join(sub_columns))
                        values.append(','.join([str(v) for v in data[row_id][header].values()]))
                    else:
                        columns.append(header)
                        values.append(data[row_id][header])
            print(f'"{row_key:^8}" | "{timestamp:^23}" | {" ,".join(columns):<45} | {{{" ,".join(values)[:50]:<50}}}')

        # Print the table header
        print(f'{"Row key":^8} | {"TimeStamp":^23} | {"Columns:Subcols":^45} | {"Value":^50}')
        print('-' * 120)

        # Print the data rows
        for row_id in json_data['rows']:
            format_row(row_id, json_data['rows'])



    # -----------------------------hbase functions-----------------------------
    def get(self, command: str) -> bool:
        command = command.replace("get", "").replace("'", "")
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

        rows = data["rows"]
        if row_key not in rows:
            print(f"\n=> Hbase::get - Row Key {row_key} not found in table {table_name}\n")
            return False

        row_data = rows[row_key]
        print(f"\n=> Hbase::get - Row Key: {row_key}\n")
        print(f"{row_key}: {row_data}")

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
        
        # checking if the table is disabled
        if self.check_string_in_file(table_name):
            print(f"\n=> Hbase::Table - {table_name} is disabled.\n")
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
            data = json.load(file)
            headers = data['headers']
            
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
        
        if len(command) < 3 :
            print(
                f"\nValue error on: {command}\nToo few arguments for alter fuction.\nUsage: alter '<table_name>', '<column_family_name>', '<column_family_action>'\n"
            )
            return False
        elif len(command) > 3 :
            print(
                f"\nValue error on: {command}\nToo many arguments for alter fuction.\nUsage: alter '<table_name>', '<column_family_name>', '<column_family_action>'\n"
            )
            return False
        
        table_name = command[0].replace("'", "")
        
        print(table_name)
        
        # checking if the table is disabled
        if self.check_string_in_file(table_name):
            print(f"\n=> Hbase::Table - {table_name} is disabled.\n")
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
                    (cf)
                    for key in data["rows"]:
                        data["rows"][key][cf] = ""
                    file.seek(0)
                    json.dump(data, file, indent=4)
                    file.truncate()
                    print(f"\n=> Hbase::Table - Added {cf} to {value}.\n")
                    print('Finished in ', time.time() - start_time, 'seconds')
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
                    print('Finished in ', time.time() - start_time, 'seconds')
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
        
        # checking if the table is disabled
        if self.check_string_in_file(command):
            print(f"\n=> Hbase::Table - {command} is disabled.\n")
            return False

        # Getting the headers of the table
        with open(f"./HbaseCollections/{command}.json", "r") as file:
            data = json.load(file)
        headers = list(data.keys())
        # Printing the results
        print(f"\nTable {command}")
        print(f"{len(headers)} column(s)")
        for header in data['headers']:
                print(header)
        # Setting the end time of the function
        end_time = time.time()
        print(f"{len(data['rows'])} column(s)")
        
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


        
        # Cargar datos de la tabla desde el archivo JSON
        with open(f"./HbaseCollections/{table_name}.json", "r") as f:
            data = json.load(f)
        
        print('Table has :',len(data['rows']), 'rows.')
        
        data['rows'] = {}
        self.disable(f"disable '{table_name}'")
        # Volver a habilitar la tabla
        self.enable(f"enable '{table_name}'")
        # Imprimir filas eliminadas
        print(f"\n=> Hbase::Table - {table_name} deleted {len(data['rows'])} rows.\n")
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
    
    def truncate(self, table_name: str) -> bool:
        # Verificar si la tabla existe
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False

        # Vaciar el archivo JSON de la tabla
        with open(f"./HbaseCollections/{table_name}.json", 'w') as file:
            file.write('{"headers":[],"rows":{}}')
        self.disable(f"disable '{table_name}'")

        print(f"\n=> Hbase::Table - {table_name} truncated.\n")
        self.enable(f"enable '{table_name}'")
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
  
    def put(self, command: str) -> bool:
        command = command.replace("put", "").replace(' ', '').replace("'","").split(",")
        command = [spec.replace("'", "") for spec in command]

        if len(command) != 4:
            print("ERROR: Wrong number of arguments")
            print("Usage: put '<table_name>', '<row_id>', '<column:subcolumn>', '<value>'")
            return False
        
        

        table_name = command[0]
        row_id = command[1]
        col_subcol = command[2].split(":")
        new_value = command[3]

        if self.check_string_in_file(table_name):
            print(f"{table_name} is disabled.")
            return False

        # load the JSON data
        with open(f'./HbaseCollections/{table_name}.json') as f:
            data = json.load(f)
        
        if col_subcol[0] not in data['headers']:
            print(f'Column name {col_subcol[0]} not found in headers.')
            return False        
        try:
            row = data['rows'][row_id]
            column = col_subcol[0]

            # update the column and subcolumn
            if column in data['headers'] and column in row:
                subcolumn = col_subcol[1]
                row[column][subcolumn] = new_value
            else:
                print(f'Column {column} added')
                row[column] = {col_subcol[1]: new_value}
        except KeyError:
            # create a new row with the specified column and subcolumn
            if col_subcol[0] in data['headers']:
                data['rows'][row_id] = {col_subcol[0]: {col_subcol[1]: new_value}}
            else:
                print(f'Column name {col_subcol[0]} not found in headers.')
                return False
                
            

        # save the updated JSON data to file
        with open(f'./HbaseCollections/{table_name}.json', 'w') as f:
            json.dump(data, f)

        return True

    def enable(self, command):
        # Setting the start time of the function
        start = time.time()
        # Removing the enable command from the command and splitting the command
        command = command.replace("enable", "").replace(' ', '').split(",")
        # Checking the command syntax
        if len(command) < 1:
            print(
                f"\nValue error on: {command}\nToo many arguments for enable function.\nUsage: enable '<table_name>'\n"
            )
            return False
        elif command[0][-1] != "'" or command[0][0] != "'":
            print(
                f"\nSyntax error on: {command[0]}\nCorrect use of single quotes is required.\nUsage: enable '<table_name>'\n"
            )
            return False
        # Getting the table name from the command
        command = command[0].replace("'", "")

        direc = './HbaseCollections'
        filename = f'{command}.json'
        file_path = os.path.join(direc, filename)

        # Checking if the table exists
        if os.path.exists(file_path):
            # Remove the name of the table from the disabled tables txt file
            with open("./disabledTables.txt", 'r') as file:
                file_contents = file.readlines()
            with open("./disabledTables.txt", 'w') as file:
                for line in file_contents:
                    if line.strip() != command:
                        file.write(line)
            # Setting the end time of the function and printing the results
            end = time.time()
            # printing the results
            print(f'0 row(s) in {round(end-start,4)} seconds')
            print(f"\n=> Hbase::Table - {command} enabled")
            return True
        else:
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False

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
        
        self.format_nested_json(data)
        
        return True

    def mainHBase(self):
        is_enabled = True
        counter = 0
        initial = input("\n[cloudera@quickstart ~]$ ")
        # time.sleep(2)
        # Start the Hbase shell
        if initial == "hbase shell":
            print(
                f" \nINFO [main] Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available\n Hbase shell enter 'help<RETURN>' for list of supported commands. Type 'exit<RETURN>' to leave the HBase Shell\n Version 1.4.13, rUnknown\n")

            
            while is_enabled:
                # User enters any command of the Hbase shell
                command = input(f"hbase(main):00{counter}:0>")
                counter += 1

                # Status commmand
                if command == 'status':
                    print(
                        '1 active master, 0 backup masters, 1 servers, 0 dead, 1.0000 average load')

                elif 'update_many' == command.split(" ")[0]:
                    self.update_many(command)
                    
                
                elif command == 'version':
                    print(
                        f'1.4.13, rJAA, {datetime.today().strftime("%Y-%m-%d")} ')

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
                    
                elif 'enable' == command.split(" ")[0]:
                    self.enable(command)

                elif 'scan' == command.split(" ")[0]:
                    self.scan(command)
                    
                elif 'alter' == command.split(" ")[0]:
                    self.alter(command)
                    
                elif 'is_enabled'== command.split(" ")[0]:
                    print(self.is_enabled(command))
                    
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
                    self.drop(table_name)
                
                elif 'describe' == command.split(" ")[0]:
                    self.describe(command)

                elif 'dropAll' == command.split(" ")[0]:
                    hbase.dropAll()

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
                    
                elif 'scan' == command.split(" ")[0]:
                    self.scan(command)

                elif command == "exit<RETURN>" or command == "exit":
                    is_enabled = False
                
                elif command == '':
                    pass
                else:
                    print(f"ERROR: Unknown command '{command}'")
                    
                
        elif initial != '':
            print(f"ERROR: Unknown command '{initial}'")

    def is_enabled(self, table_name):
        table_name = table_name.replace("is_enabled", "").replace(' ', '').replace("'",'')
        with open('./disabledTables.txt', 'r') as file:
            for line in file:
                if table_name in line:
                    return False
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
    
def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')


hbase = HbaseSimulator()
# clear_screen()
hbase.mainHBase()


