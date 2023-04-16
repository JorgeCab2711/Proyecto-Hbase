import time
from datetime import datetime
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
    
    def create(self):
        pass
    
    def list_(self):
        pass
    
    def mainHBase(self):
        counter = 0
        initial = input("\n\n\n\n\n\n[cloudera@quickstart ~]$ ")
        # time.sleep(2)
        # Start the Hbase shell
        if initial == "hbase shell":
            print(f"{datetime.today().strftime('%Y-%m-%d')} \nINFO [main] Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available\n Hbase shell enter 'help<RETURN>' for list of supported commands. Type 'exit<RETURN>' to leave the HBase Shell\n Version 1.4.13, rUnknown\n")
            while True:
                # Enter any command of the Hbase shell
                command = input(f"hbase(main):00{counter}:0>")
                counter += 1
                
                # Status commmand
                if command == 'status':
                    print('1 active master, 0 backup masters, 1 servers, 0 dead, 1.0000 average load')
                
                # Version command
                elif command == 'version':
                    print(f'1.4.13, rUnknown, {datetime.today().strftime("%Y-%m-%d")}')
                
                # TODO table help command
                elif command == 'table_help':
                    pass
                # whoami command
                elif command == "whoami":
                    print("cloudera (auth:SIMPLE)\n     groups: cloudera, default")
                
                elif 'create' in command:
                    # TODO Implement create table function 
                    pass
                
                
                    
        
            
hbase = HbaseSimulator()
hbase.mainHBase()