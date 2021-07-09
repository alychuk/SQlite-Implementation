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
            match = re.match(r'^(?!(\.)$)\w+$', i)
            if match:
                self.databases.append(match[0])

    class Table(object):
        def __init__(self, tbl_name):
            self.name = tbl_name
            self.fields = []
            self.tuples = []

        def read_table(self, db_name):
            file = open(os.path.join(os.getcwd(), db_name, self.name + ".txt"), "r")
            table = file.readlines()
            self.name = table[0].replace('\n', '')
            #remove new line characters
            temp = table[1].replace('\n', '')
            self.fields = temp.split(" | ")
            for i in table[2:]:
                temp = i.replace('\n', '')
                self.tuples.append(temp.split(" | "))
            file.close()

        def write_table(self, db_name):
            if os.path.exists(self.name + ".txt"):
                os.remove(self.name + ".txt")
            else:
                file = open(os.path.join(os.getcwd(), db_name, self.name + ".txt"), "w")
                file.write(self.name + '\n')
                file.write(" | ".join(self.fields) + '\n')
                for i in self.tuples:
                    file.write(" | ".join(i) + '\n')
                file.close()

        def insert(self, db_name, tuple):
            file = open(os.path.join(os.getcwd(), db_name, self.name + ".txt"), "a")
            file.write(" | ".join(tuple) + '\n')
            file.close()

        #takes in a tuple (name,type)
        def add_field(self, field):
            self.fields.append(field)

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


    def select(self, tname, field, where = ()):
        tname = tname.lower()
        if self.current_database != None and (tname in (table.lower() for table in self.current_tables)):
            table = self.Table(tname.capitalize())
            table.read_table(self.current_database)
            if field[0] == "*":
                print(" | ".join(table.fields))
                for tuple in table.tuples:
                    print(" | ".join(tuple))
            else:
                where_index = -1
                field_index = []
                printed_first = True
                for i in field:
                    for cnt, j in enumerate(table.fields):
                        if re.match(where[0], j) and where_index == -1:
                            where_index = cnt
                        #check if field exists
                        if re.match(i,j):
                            field_index.append(cnt)
                            if printed_first == True:
                                print(j, end = '')
                                printed_first = False
                            else:
                                print(" | ", j, end = '')
                print("")
                for tuple in table.tuples:
                    if where[1] == '!=' and where_index != -1:
                        if tuple[where_index] != where[2]:
                            for cnt, i in enumerate(field_index):
                                if cnt == 0:
                                    print(tuple[i], end = '')
                                else:
                                    print(" | ",tuple[i], end = '')
                            print("")
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
            else:
                print("!Failed to alter table ", tname, " because it does not exist.")

    def insert_into(self, tname, tuples):
        if self.current_database != None and (tname in self.current_tables):
            table = self.Table(tname)
            table.insert(self.current_database, tuples)
            print("1 new record inserted.")
        else:
            print("!Failed to insert into table ", tname, " because it does not exist.")

    def update(self, tname, set, where):
        tname = tname.lower()
        if self.current_database != None and (tname in (table.lower() for table in self.current_tables)):
            table = self.Table(tname.capitalize())
            table.read_table(self.current_database)
            set_col = -1
            where_col = -1
            # find what column we are matching and what column we are setting
            for cnt, col in enumerate(table.fields):
                if re.match(set[0], col):
                    set_col = cnt
                if re.match(where[0], col):
                    where_col = cnt
            if set_col != -1 and where_col != -1:
                records_updated = 0
                for row in table.tuples:
                    if row[where_col] == where[1]:
                        row[set_col] = set[1]
                        records_updated = records_updated + 1
                table.write_table(self.current_database)
                print(records_updated, " record modified.")
            else:
                print("!Field ", set[0], " or ", where[0], " does not exist.")
        else:
            print("!Failed to update table ", tname, " because it does not exist.")

    def delete_from(self, tname, field, operator, value):
        tname = tname.lower()
        if self.current_database != None and (tname in (table.lower() for table in self.current_tables)):
            table = self.Table(tname.capitalize())
            table.read_table(self.current_database)
            col_num = -1
            for cnt, col in enumerate(table.fields):
                if re.match(field, col):
                    col_num = cnt
            if col_num != -1:
                to_pop = []
                for cnt, row in enumerate(table.tuples):
                    if operator == '=':
                        if row[col_num] == value:
                            to_pop.append(cnt)
                    else:
                        if operator == '<' and value.isdigit():
                            if float(row[col_num]) < float(value):
                                to_pop.append(cnt)
                        elif operator == '>' and value.isdigit():
                            if float(row[col_num]) > float(value):
                                to_pop.append(cnt)
                        else:
                            print("!Invalid operator.")
                records_deleted = len(to_pop)
                for cnt, i in enumerate(to_pop):
                    table.tuples.pop(i-cnt)
                table.write_table(self.current_database)
                print(records_deleted, " records deleted.")
            else:
                print("!Field ", field, " does not exist.")
        else:
            print("!Failed to delete from table ", tname, " because it does not exist.")
