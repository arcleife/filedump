import MySQLdb
import csv
import os
import glob
from datetime import datetime, timedelta
import pandas as pd

def PopulateData(data, id):
    output = []
    for f in data:
        with open(f, newline='') as csvfile:
            sensor_reader = csv.DictReader(csvfile)
            for row in sensor_reader:
                output.append({'Time': row['Time'], 
                               'Temp': row['Temp ch1'], 
                               'Pressure': row['Pressure ch1'], 
                               'Humidity': row['Humidity ch1'], 
                               'Temp_id': id[0], 
                               'Pressure_id': id[1], 
                               'Humidity_id': id[2]})
    return output

def ExportToCsv(data, filename):
    if not os.path.exists('export'):
        os.makedirs('export')
    with open("export/"+filename, 'w', newline='') as csvfile:
        fieldnames = ['Time', 'Temp', 'Pressure', 'Humidity', 'Temp_id', 'Pressure_id', 'Humidity_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in data:
            writer.writerow(row)

def ExportToCsvLaravel(data, filename):
    if not os.path.exists('export'):
        os.makedirs('export')
    with open("export/"+filename, 'w', newline='') as csvfile:
        fieldnames = ['sensor_id', 'taken_at', 'value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in data:
            writer.writerow({'sensor_id': row['Temp_id'], 'taken_at': row['Time'], 'value': row['Temp'],})
            writer.writerow({'sensor_id': row['Pressure_id'], 'taken_at': row['Time'], 'value': row['Pressure'],})
            writer.writerow({'sensor_id': row['Humidity_id'], 'taken_at': row['Time'], 'value': row['Humidity'],})

def ImportFromCsv(filename):
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def WriteToDatabase(data):
    hostname = '127.0.0.1'
    username = 'root'
    password = 'secret'
    database = 'anomaly_db'

    myConnection = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database)
    cur = myConnection.cursor()

    for row in data:
        if (row['Temp'] != ''):
            val = (row["Temp_id"], row['Time'], row['Temp'])
            cur.execute( "INSERT INTO sensor_values(sensor_id, taken_at, value) VALUES (%s, %s, %s);", val)
        if (row['Pressure'] != ''):
            val = (row["Pressure_id"], row['Time'], row['Pressure'])
            cur.execute( "INSERT INTO sensor_values(sensor_id, taken_at, value) VALUES (%s, %s, %s);", val)
        if (row['Humidity'] != ''):
            val = (row["Humidity_id"], row['Time'], row['Humidity'])
            cur.execute( "INSERT INTO sensor_values(sensor_id, taken_at, value) VALUES (%s, %s, %s);", val)
    myConnection.commit()
    myConnection.close()

def GetFromDatabase(time, id):
    # Change to server's database when deployed
    hostname = '127.0.0.1'
    username = 'admin'
    password = '#anomaly1'
    database = 'anomaly_db'

    myConnection = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database)
    cur = myConnection.cursor()

    time_until = time
    time_from = time_until - timedelta(hours=1)
    current_time = time_until.strftime("%Y-%m-%d %H:%M:%S")
    last_hour_time = time_from.strftime("%Y-%m-%d %H:%M:%S")

    val = (last_hour_time, current_time, id)
    
    cur.execute( "SELECT taken_at, value FROM sensor_values WHERE taken_at > %s AND taken_at < %s AND sensor_id = %s ORDER BY taken_at ASC;", val)
    rows = cur.fetchall()

    myConnection.commit()
    myConnection.close()

    return rows
    
def GetAndExport(filename, time=""):
    # time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    time = datetime.now()

    # Change to 7, 8, 9 when deployed
    data_temp = GetFromDatabase(time, 7)
    data_press = GetFromDatabase(time, 8)
    data_hum = GetFromDatabase(time, 9)

    df_temp = pd.DataFrame(data_temp, columns=['Time', 'Temp ch1'])
    df_press = pd.DataFrame(data_press, columns=['Time', 'Pressure ch1'])
    df_hum = pd.DataFrame(data_hum, columns=['Time', 'Humidity ch1'])

    df_temp['Time'] = df_temp['Time'].dt.strftime('%Y/%m/%d %H:%M')
    df_press['Time'] = df_press['Time'].dt.strftime('%Y/%m/%d %H:%M')
    df_hum['Time'] = df_hum['Time'].dt.strftime('%Y/%m/%d %H:%M')

    output = df_temp.join ([df_press['Pressure ch1'], df_hum['Humidity ch1']])

    output.to_csv(filename, index=False)

def RecordAnomaly(time):
    hostname = '127.0.0.1'
    username = 'root'
    password = 'secret'
    database = 'anomaly_db'

    myConnection = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database)
    cur = myConnection.cursor()

    time_until = time
    time_from = time_until - timedelta(hours=1)
    current_time = time_until.strftime("%Y-%m-%d %H:%M:%S")
    last_hour_time = time_from.strftime("%Y-%m-%d %H:%M:%S")

    val = (current_time, last_hour_time)
    cur.execute( "INSERT INTO anomalies(occur_from, occur_until) VALUES (%s, %s);", val)
    
    myConnection.commit()
    myConnection.close()

            
if __name__ == "__main__":
    # Uncomment for updating the data
    # extension = 'csv'
    # path = '\\.'
    # os.listdir()
    # files = glob.glob('*.{}'.format(extension))

    # id_b = [1, 2, 3]
    # id_bplus = [4, 5, 6]

    # files_b = [f for f in files if len(f.split("_")) > 1]
    # files_bplus = [f for f in files if len(f.split("_")) == 1]

    # b = PopulateData(files_b, id_b)
    # bplus = PopulateData(files_bplus, id_bplus)

    # ExportToCsv(b, "b.csv")
    # ExportToCsv(bplus, "bplus.csv")

    # ExportToCsvLaravel(b, "testing_b.csv")
    # ExportToCsvLaravel(bplus, "testing_bplus.csv")

    # WriteToDatabase(b)
    # WriteToDatabase(bplus)

    #GetAndExport('output.csv')
    #RecordAnomaly(datetime.now())