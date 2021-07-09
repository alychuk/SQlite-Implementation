#CS457 UNR
#Author: Adam Lychuk
import os
import shutil
import re

class DataManager(object):
    def __init__(self):
        self.databases = []
        self.current_tables = []
        self.current_database = None
        self.__get_databases()

    #gets the current list of databases
    def __get_databases(self):
        self.databases.clear()
        #search for all file directories and append them to a list
        for i in os.listdir():
            match = re.match(r'^(?!(\.)$)[db]\w+$', i)
            if match:
                self.databases.append(match[0])

    class Table(object):
        def __init__(self, tbl_name):
            self.name = tbl_name
            self.fields = []
            self.tuples = {}

        def read_table(self, db_name):
            file = open(os.path.join(os.getcwd(), db_name, self.name + ".txt"), "r")
            table = file.readlines()
            self.name = table[0].replace('\n', '')
            #remove new line characters
            temp = table[1].replace('\n', '')
            self.fields = temp.split(" | ")
            file.close()

        def write_table(self, db_name):
            if os.path.exists(self.name + ".txt"):
                os.remove(self.name + ".txt")
            else:
                file = open(os.path.join(os.getcwd(), db_name, self.name + ".txt"), "w")
                file.write(self.name + '\n')
                file.write(" | ".join(self.fields) + '\n')
                file.close()

        #takes in a tuple (name,type)
        def add_field(self, field):
            self.fields.append(field)

        def select(self, symbol):
            if symbol == "*":
                print(" | ".join(self.fields))

    def __get_tables(self, db_name):
        self.current_tables.clear()
        db_path = os.path.join(os.getcwd(), db_name)
        for i in os.listdir(db_path):
            match = re.match(r'([a-zA-Z0-9\s_\\.\-\(\):]+)(.txt)$', i)
            if match:
                self.current_tables.append(match.group(1))

    def create_database(self, database):
        if database in self.databases:
            print("!Failed to create ", database," because it already exists.")
        else:
            new_path = os.path.join(os.getcwd(), database)
            os.mkdir(new_path)
            self.databases.append(database)
            print("Database ", database, " created.")

    def drop_database(self, database):
        if database in self.databases:
            drop_path = os.path.join(os.getcwd(), database)
            shutil.rmtree(drop_path)
            self.databases.remove(database)
            print("Database ", database, " deleted.")
        else:
            print("!Failed to delete ", database, " because it does not exist.")

    def use_database(self, database):
        if database in self.databases:
            self.current_database = database
            self.__get_tables(database)
            print("Using database ", database, ".")
        else:
            print("!Failed to use ", database, " because it does not exist")

    def create_table(self, tbl):
        tname, fields = tbl
        if self.current_database != None and (tname in self.current_tables):
            print("!Failed to create table ", tname, " because it already exists.")
        else:
            table = self.Table(tname)
            for i in range(0, len(fields), 2):
                table.add_field(fields[i] + " " + fields[i+1])
            table.write_table(self.current_database)
            self.__get_tables(self.current_database)
            print("Table ", tname, " created.")

    def drop_table(self, tname):
        if self.current_database != None and (tname in self.current_tables):
            os.remove(os.path.join(os.getcwd(), self.current_database, tname + ".txt"))
            print("Table ", tname, " deleted.")
            self.__get_tables(self.current_database)
        else:
            print("!Failed to delete ", tname, " because it does not exist.")


    def select(self, tname):
        if self.current_database != None and (tname in self.current_tables):
            table = self.Table(tname)
            table.read_table(self.current_database)
            table.select('*')
        else:
            print("!Failed to query table ", tname, " because it does not exist.")


    def alter_table(self, tname, kword, field):
        if self.current_database != None and (tname in self.current_tables):
            if kword == "ADD":
                # i need to open the file and rewrite, no seek since the data is new
                table = self.Table(tname)
                table.read_table(self.current_database)
                table.add_field(field)
                table.write_table(self.current_database)
                #table.select('*')
                print("Table ", tname, " modified.")
