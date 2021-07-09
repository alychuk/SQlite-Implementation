#CS457 UNR
#Author: Adam Lychuk
import sys
import re
from parser import Parser

def main():
    parser = Parser()
    # Since argv is 1 expect standard input
    if len(sys.argv) == 1:
        print("Enter command: ")
        for line in sys.stdin:
            parser.run_commands(line.rstrip())
            print("Enter command: ")
    # expect file name in command line argument
    else:
        parser.run_commands(" ".join(sys.argv))

if __name__ == "__main__":
    main()
