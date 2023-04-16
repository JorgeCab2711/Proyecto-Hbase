import time
from datetime import datetime
import pandas as pd
import csv
import os


class HbaseSimulator:
    def __init__(self) -> None:
        self.IP = "198.167.0.1"
        self.tables = {}

    def put(self):
        pass

    def get(self):
        pass

    def scan(self):
        pass

    def delete(self):
        pass

    def deleteAll(self):
        pass

    def count(self):
        pass

    def truncate(self):
        pass

    def disable(self):
        pass

    def is_enabled(self):
        pass

    def alter(self):
        pass

    def drop(self):
        pass

    def drop_all(self):
        pass

    def describe(self):
        pass

    def create(self, command: str) -> bool:
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

        print(f"\n=> Hbase::Table - {value} created")

        return True

    def list_(self):

        files = os.listdir("./HbaseCollections")
        for file in files:
            if os.path.isfile(os.path.join("./HbaseCollections", file)):
                print(file.replace(".csv", ""))

    def mainHBase(self):
        counter = 0
        initial = input("\n\n\n\n\n\n[cloudera@quickstart ~]$ ")
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

                elif 'create' == command.split(" ")[0]:
                    # TODO Implement create table function
                    self.create(command)


hbase = HbaseSimulator()
# hbase.mainHBase()

# create 'empleado', 'nombre', 'ID', 'puesto'
# command = input('command test> ')
# hbase.create(command)
hbase.list_()