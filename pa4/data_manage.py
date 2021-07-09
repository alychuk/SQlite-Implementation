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
        self.transaction = False
        # 0 if no transaction, 1 if transaction success, 2, transaction failure
        self.transaction_aborted = 0
        self.modified_tables = []
        self.access_list = []
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

        def print_fields(self):
            print(" | ".join(self.fields), end='')

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


    def select(self, tname, field, where = (), join = None):
        if isinstance(tname,()) and self.current_database != None:
            tables = []
            for i in tname:
                if i.lower() in (table.lower() for table in self.current_tables):
                    temp = self.Table(i)
                    temp.read_table(self.current_database)
                    tables.append(temp)
            if join == None:
                tables[0].print_fields()
                print(" | ", end = "")
                tables[1].print_fields()
                print("")
                for i in tables[0].tuples:
                    for j in tables[1].tuples:
                        if i[0] == j[0]:
                            print((" | ").join(i), end = "")
                            print(" | ", end = "")
                            print((" | ").join(j))
            elif re.match(r'inner join', join):
                tables[0].print_fields()
                print(" | ", end = "")
                tables[1].print_fields()
                print("")
                for i in tables[0].tuples:
                    for j in tables[1].tuples:
                        if i[0] == j[0]:
                            print((" | ").join(i), end = "")
                            print(" | ", end = "")
                            print((" | ").join(j))
            elif re.match(r'left outer join', join):
                tables[0].print_fields()
                print(" | ", end = "")
                tables[1].print_fields()
                print("")
                matches = []
                for cnt1, i in enumerate(tables[0].tuples):
                    for cnt2, j in enumerate(tables[1].tuples):
                        if i[0] == j[0]:
                            matches.append((cnt1, cnt2))
                j = 0
                for idx, i in enumerate(matches):
                    if i[0] == j:
                        print((" | ").join(tables[0].tuples[j]), end = "")
                        print(" | ", end = "")
                        print((" | ").join(tables[1].tuples[i[1]]))
                        if idx+1 < len(matches):
                            j = matches[idx+1][0]
                if j < len(tables[0].tuples):
                    for i in range(j+1,len(tables[0].tuples)):
                        print((" | ").join(tables[0].tuples[i]), end = "")
                        print(" | ", end = "")
                        print(" | ")

        else:
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
        if self.transaction == True:
            if self.__check_for_lock(tname.capitalize()):
                if tname.capitalize() in self.access_list:
                    self.transaction_aborted = 1
                else:
                    self.transaction_aborted = 2
            else:
                self.access_list.append(tname.capitalize())
                self.__add_lock(tname.capitalize())
                self.transaction_aborted = 1
        else:
            self.transaction_aborted = 0
        if self.transaction_aborted == 1 or self.transaction_aborted == 0:
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
                    if self.transaction_aborted == 0:
                        table.write_table(self.current_database)
                        print(records_updated, " record modified.")
                    elif self.transaction_aborted == 1:
                        self.modified_tables.append((table, str(records_updated) + " record modified." ))
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

    def begin_transaction(self):
        self.transaction = True
        print("Transaction starts.")

    def commit(self):
        self.transaction = False
        if self.modified_tables != None:
            if self.transaction_aborted == 2:
                print("Transaction abort.")
                self.transaction_aborted = 0
                self.modified_tables = None
            elif self.transaction_aborted == 1:
                self.transaction_aborted = 0
                for i in self.modified_tables:
                    i[0].write_table(self.current_database)
                    print(i[1])
                    self.__remove_lock(i[0].name)
                print("Transaction committed.")
        else:
            print("No changes made.")

    def __check_for_lock(self, tname):
        self.__get_tables(self.current_database)
        lock_name = tname + "__lock"
        if lock_name in self.current_tables:
            print("Error: Table ", tname, "is locked!")
            return True
        else:
            return False

    def __add_lock(self, tname):
        file = open(os.path.join(os.getcwd(), self.current_database, tname + "__lock" + ".txt"), "w")
        file.close()

    def __remove_lock(self, tname):
        if os.path.exists(os.path.join(os.getcwd(), self.current_database, tname + "__lock" + ".txt")):
            os.remove(os.path.join(os.getcwd(), self.current_database, tname + "__lock" + ".txt"))
