import csv
import sqlite3



def csv_reading(name):
    path = "data/" + name

    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print(row)