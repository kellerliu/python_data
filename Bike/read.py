
"""
-J , Feb 7 2015, 11:59pm

Schema use python
Schema 2 uses python 3.4    """

import csv
import sys
import datetime
#import MySQLdb, this is not available
import sqlite3


conn = sqlite3.connect('data.db')
c = conn.cursor()


# Create table, only run once
#c.execute("DROP TABLE IF EXISTS bikeUsage")

c.execute('''CREATE TABLE bikeUsage
    (bikeid text, end_station_id text, start_station_id text, starttime text, stoptime text)''')


#with open('data_small.csv', 'r', newline='') as csvfile:

with open('data.csv', 'r', newline='') as csvfile: #1:07pm - 1:17pm
    """
    comment out this for now, as it takes very long :)
    But here's the result:
    total number of data rows: 1802997  total time: 18496 days, 15:18:02  ave time: 0:14:46.362807  """
    
    spamreader = csv.reader(csvfile, delimiter = ',')
    count = 0
    total = datetime.timedelta(0)
    header = next(spamreader)
    print("header:", header)
    for row in spamreader:
        count += 1
        if len(row) < 5:
            print("data row", count, "not complete", len(row))
        else:
            dt3 = datetime.datetime.strptime (row[3], "%Y-%m-%d %H:%M:%S")
            dt4 = datetime.datetime.strptime (row[4], "%Y-%m-%d %H:%M:%S")
            useTime = dt4-dt3
            total += useTime
            print ("data row", count, "bikeId", row[0], header[4], "-", header[3], useTime)
            # Insert a row of data
            c.execute("INSERT INTO bikeUsage VALUES ('"+row[0]+"','"+row[1]+"','"+row[2]+"','"+row[3]+"', '"+row[4]+"')")

    print("total number of data rows:", count, " total time:", total, " ave time:", total/count)

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()


