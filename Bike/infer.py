import datetime
import sqlite3

# How station fe2a5f differs from station fec8ff?

conn = sqlite3.connect('data.db')
c = conn.cursor()
"""
c.execute("SELECT bikeid, end_station_id, start_station_id, starttime, stoptime FROM bikeUsage WHERE start_station_id = :sId ", {sId: 'fe2a5f'}) """

c.execute("SELECT COUNT() FROM bikeUsage WHERE start_station_id = :sId ", {"sId": 'fe2a5f'}) #9598

rows = c.fetchall()
for row in rows:
    print (row)

c.execute("SELECT COUNT() FROM bikeUsage WHERE start_station_id = :sId ", {"sId": 'fec8ff'}) #2921

rows = c.fetchall()
for row in rows:
    print (row)

c.execute("SELECT COUNT() FROM bikeUsage WHERE end_station_id = :sId ", {"sId": 'fe2a5f'}) #10492

rows = c.fetchall()
for row in rows:
    print (row)

c.execute("SELECT COUNT() FROM bikeUsage WHERE end_station_id = :sId ", {"sId": 'fec8ff'}) #3362

rows = c.fetchall()
for row in rows:
    print (row)
