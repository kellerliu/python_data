import datetime
import sqlite3


conn = sqlite3.connect('data.db')
c = conn.cursor()

"""
c.execute("SELECT COUNT() FROM bikeUsage WHERE bikeid = '884f5b30'") #288

rows = c.fetchone()
for row in rows:
    print (row) """

"""
bikeid = '036ca6e3'
c.execute("SELECT bikeid, end_station_id, start_station_id, starttime, stoptime FROM bikeUsage WHERE bikeid = :Id ORDER BY starttime", {"Id": bikeid})

missing = 0
rowCount = 0
prevEndStation = ''

rows = c.fetchall()
for row in rows:
    rowCount += 1
    if prevEndStation != row[2]:
        missing += 1
    
    print (row)
    prevEndStation = row[1]
print("totalRow", rowCount, "missing", missing, "ratio", missing/rowCount) """ #0.125 0.1366

"""
c.execute("SELECT COUNT(DISTINCT bikeid) FROM bikeUsage")
rows = c.fetchone()
print (rows) # 6549 """


"""
c.execute("SELECT DISTINCT bikeid FROM bikeUsage") # ~30min
rowCount = 0
missing = 0

idRows = c.fetchall()
for idRow in idRows:
    bikeid = idRow[0]


    c.execute("SELECT bikeid, end_station_id, start_station_id, starttime, stoptime FROM bikeUsage WHERE bikeid = :Id ORDER BY starttime", {"Id": bikeid})

    prevEndStation = ''
    rows = c.fetchall()
    for row in rows:
        rowCount += 1
        if prevEndStation != row[2]:
            missing += 1
    
        print (row)
        prevEndStation = row[1]

print("totalRow", rowCount, "missing", missing, "ratio", missing/rowCount)


#totalRow 1802997 missing 222141 ratio 0.12320652779788319, 
#this one is wrong, the first row is counted,
#so counts 6549 more.

    """



c.execute("SELECT bikeid, end_station_id, start_station_id, starttime, stoptime FROM bikeUsage ORDER BY bikeid, starttime ") #4:08 pm

rowCount = 0
missing = 0
prevEndStation = ''
bikeId = ''

rows = c.fetchall()
for row in rows:
    rowCount += 1
    
    if bikeId == row[0] and prevEndStation != row[2] :
        missing += 1
    
    print (row)
    bikeId = row[0];
    prevEndStation = row[1]
print("counted Row", rowCount, "missing", missing, "ratio", missing/(rowCount+ missing))


#totalRow 1802997 missing 215592 ratio 0.10680331657410201 """





