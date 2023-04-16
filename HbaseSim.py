import time
from datetime import datetime
import pandas as pd
import csv
import os


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
        file_names = [file_name.replace(".csv", "")
                      for file_name in file_names]
        return file_names

    # Function to get the number of rows in a table
    def count_rows(self, table_name: str) -> int:
        with open(f'./HbaseCollections/{table_name}.csv', 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)
            count = 0
            for row in reader:
                count += 1
        return count

    def check_string_in_file(self, search_string):
        with open('./disabledTables.txt', 'r') as file:
            for line in file:
                if search_string in line:
                    return True
        return False

    # -----------------------------hbase functions-----------------------------

    def put(self):
        pass

    def get(self):
        pass

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

        with open(f'./HbaseCollections/{command}.csv', 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)
            print("{:<10} {:<30}".format("ROW", "COLUMN+CELL"))
            for row in reader:
                if len(headers) != len(row):
                    print('\n')
                    return False
                row_key = row[0]
                row_str = "{:<10} ".format(row_key)
                column_cell_str = ""
                for i in range(1, len(headers)):
                    column_cell_str += "{}: ={}, ".format(headers[i], row[i])
                column_cell_str = column_cell_str.rstrip(', ')
                print(row_str + column_cell_str)

        return True

    def delete(self):
        pass

    def deleteAll(self):
        pass

    def count(self, table_name: str, search_param: str = None) -> int:
        # Verificar si la tabla existe
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False

        with open(f'./HbaseCollections/{table_name}.csv', 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)

            # Contar el total de filas
            if search_param is None:
                count = 0
                for row in reader:
                    count += 1
                print(f"\n=> Hbase::Table - {table_name} has {count} rows.\n")
                return count

            # Contar solo filas que coinciden con el parÃÂ¡metro de bÃÂºsqueda
            else:
                row_count = 0
                for row in reader:
                    if search_param in row:
                        row_count += 1
                print(f"\n=> Hbase::Table - {table_name} has {row_count} rows that match the search parameter '{search_param}'.\n")
                return row_count
        

    def truncate(self):
        pass

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
        filename = f'{command}.csv'
        file_path = os.path.join(direc, filename)

        # Checking if the table exists
        if os.path.exists(file_path):
            # Write the name of the table on the disabled tables txt file
            with open("./disabledTables.txt", 'w') as file:
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

    def is_enabled(self):
        pass

    def alter(self):
        pass

    def drop(self, table_name: str) -> bool:
        
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False
        os.remove(f'./HbaseCollections/{table_name}.csv')
        self.table_names.remove(table_name)
        print(f"\n=> Hbase::Table - {table_name} dropped.\n")
        return True
    
    def dropAll(self):
        for table_name in self.table_names:
            self.drop(table_name)
            os.remove(f'./HbaseCollections/{table_name}.csv')
            self.table_names = []
            print("\n=> Hbase::All tables dropped\n")
            return True
            
        
        
    def describe(self):
        pass

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
        rowKey = len(self.tables) + 1

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
        meta_data = {}
        meta_data['Row Key'] = rowKey
        meta_data['Timestamp'] = timestamp
        meta_data['Value'] = value

        # removing the table name from the list
        command.remove(value)

        # Adding the meta data to the default table
        # If the table already exists, it will be updated, else it will be created
        if not os.listdir("./HbaseCollections"):
            filename = os.path.join("./HbaseCollections", f"TABLE.csv")
            df = pd.DataFrame(meta_data, index=[0])
            df.to_csv(filename, index=False)
        else:
            filename = os.path.join('./HbaseCollections', 'TABLE.csv')
            df = pd.DataFrame(meta_data, index=[0])
            df.to_csv(filename, mode='a', header=False, index=False)

        # Adding the headers to de HFile
        with open(f"./HbaseCollections/{value}.csv", 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(command)

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
                print(file.replace(".csv", ""))

    def mainHBase(self):
        counter = 0
        initial = input("\n[cloudera@quickstart ~]$ ")
        # time.sleep(2)
        # Start the Hbase shell
        if initial == "hbase shell":
            print(
                f"{datetime.today().strftime('%Y-%m-%d')} \nINFO [main] Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available\n Hbase shell enter 'help<RETURN>' for list of supported commands. Type 'exit<RETURN>' to leave the HBase Shell\n Version 1.4.13, rUnknown\n")

            command = ""
            while command != "exit<RETURN>" or command != "exit":
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

                # List tables command
                elif command == 'list':
                    self.list_()

                # Disable table command
                elif 'disable' == command.split(" ")[0]:
                    self.disable(command)

                elif 'scan' == command.split(" ")[0]:
                    self.scan(command)
                
                elif 'count' == command.split(" ")[0]:
                    #conseguir el nombre de la tabla y el nombre de la columna
                    args = command.split(" ")
                    if len(args) < 2 or len(args) > 3:
                        print("Usage: count '<table_name>' [, '<search_string>']")
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
                    
                elif command == 'drop_all':
                    hbase.drop_all()

                elif command != '':
                    print(f"ERROR: Unknown command '{command}'")
        elif initial != '':
            print(f"ERROR: Unknown command '{initial}'")


def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')


hbase = HbaseSimulator()
clear_screen()
# hbase.mainHBase()

# create 'empleado', 'nombre', 'ID', 'puesto'
# command = input('command test> ')
# hbase.create(command)
# hbase.list_()
# hbase.disable(command)
#hbase.disable("disable 'empleado'")
#hbase.scan("scan 'empleado'")
#hbase.count('empleado') #contar la cantidad de filas de la tabla
#hbase.count('empleado', 'nombre') #Busquedas que coinciden un parametro de busqueda