#CS457 UNR
#Author: Adam Lychuk
from data_manage import DataManager
import re
import os

# Class that parses user input for both files and standard input
class Parser():
    def __init__(self):
        self.data_manager = DataManager()
        self.file_input = None

    # catch all function to run list of commands
    def run_commands(self, str):
        file_path = re.search(r'\w+[.]\w+$', str)
        if (file_path):
            # if there is a file path call helper to read all commands into a list
            command_list = self.__read_commands_from_file(file_path)
            self.file_input = True
        else:
            # if no file path read indivisual command, or set of commands in a line
            command_list = self.__read_command(str)
            self.file_input = False

        # Based on command call data_manager method to handle
        for i in command_list:
            if re.match(r'CREATE', i) or re.match(r'create', i):
                if re.search(r'DATABASE', i):
                    # split on spaces and return db_name
                    self.data_manager.create_database(i.split(" ")[2])
                elif re.search(r'TABLE', i) or re.search(r'table', i):
                    start = 0
                    temp = i.replace(",", "")
                    for j in range( len(temp) ):
                        if temp[j] == '(':
                            if start == 0:
                                start = j + 1
                    temp2 = i.replace('(', " ")
                    self.data_manager.create_table( (temp2.split(" ")[2], temp[start:-1].split(" ") ) )
            elif re.match(r'DROP', i):
                if re.search(r'DATABASE', i):
                    self.data_manager.drop_database(i.split(" ")[2])
                elif re.search(r'TABLE', i):
                    self.data_manager.drop_table(i.split(" ")[2])
            elif re.match(r'USE', i):
                self.data_manager.use_database(i.split(" ")[1])
            elif re.match(r'select', i):
                # first always table name, field selection, third where if it exists
                # last join type
                if i.split(" ")[1] == "*":
                    if len(i.split(" ")) < 4:
                        tokens = i.split(" ")
                        self.data_manager.select(tokens[3], tokens[1])
                    elif re.search(r'inner join', i):
                        temp = temp.replace(".", " ")
                        tokens = temp.split(" ")
                        self.data_manager.select((tokens[3], tokens[5]), tokens[1],
                                                    (tokens[9], tokens[10], tokens[12]),
                                                    "inner join")
                    elif re.search(r'left outer join', i):
                        temp = temp.replace(".", " ")
                        tokens = temp.split(" ")
                        self.data_manager.select((tokens[3], tokens[5]), tokens[1],
                                                    (tokens[9], tokens[10], tokens[12]),
                                                    "left outer join")
                    else:
                        temp = i.replace(", ", " ")
                        temp = temp.replace(".", " ")
                        tokens = temp.split(" ")
                        self.data_manager.select((tokens[3], tokens[5]), tokens[1],
                                                    (tokens[9], tokens[10], tokens[12]))
                else:
                    temp = i.replace(",", "")
                    tokens = temp.split(" ")
                    self.data_manager.select(tokens[4], (tokens[1], tokens[2]),
                                                (tokens[6], tokens[7], tokens[8]))
            elif re.match(r'ALTER TABLE', i):
                tokens = i.split(" ")
                self.data_manager.alter_table(tokens[2], tokens[3], tokens[4] + " " + tokens[5])
            elif re.match(r'insert into', i):
                start = 0
                temp = i.replace(" ", "")
                temp = temp.replace(",", " ")
                temp = temp.replace("\t", "")
                temp = temp.replace("\'", "")
                for j in range( len(temp) ):
                    if temp[j] == '(':
                        if start == 0:
                            start = j + 1
                tokens = i.split(" ")
                self.data_manager.insert_into(tokens[2], temp[start:-1].split(" "))
            elif re.match(r'update', i):
                # handle both spaced and unspaced =
                temp = i.replace(" = "," ")
                temp = temp.replace("=", " ")
                temp = temp.replace("\'", "")
                tokens = temp.split(" ")
                self.data_manager.update(tokens[1], (tokens[3],tokens[4]), (tokens[6],tokens[7]))
            elif re.match(r'delete', i):
                temp = i.replace("\'", "")
                tokens = temp.split(" ")
                self.data_manager.delete_from(tokens[2], tokens[4], tokens[5], tokens[6])
            elif re.match(r'.EXIT', i):
                print("All done")
                if self.file_input == True:
                    break;
                else:
                    exit(0)
            elif re.match(r'.exit', i):
                print("All done")
                if self.file_input == True:
                    break;
                else:
                    exit(0)
            elif re.match(r'--', i):
                print("You've entered a comment not command.")
            elif re.match(r'', i):
                print("No command entered.")

    def __read_commands_from_file(self, file_path):
            file = open(os.path.join(os.getcwd(), file_path.group(0)), "r")
            lines = file.read()
            file.close()
            # removes comments starting with --
            lines = re.sub(r'(?m)^\-\-.*\n?', '', lines)
            # removes all extra spacing
            lines = lines.replace('\n', '')
            # splits along semi colons so we have list of command
            return lines.split(";")

    def __read_command(self, str):
        user_input_txt = re.sub(r'(?m)^\-\-.*\r?', '', str)
        user_input_txt = user_input_txt.replace('\n', ' ')
        command_list = user_input_txt.split(";")
        for cnt, i in enumerate(command_list):
            command_list[cnt] = i.lstrip()
        print(command_list, "???????????????")
        print("????????????????")
        return command_list
