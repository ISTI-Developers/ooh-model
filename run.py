from datetime import datetime, timedelta 
import json
import time
import predict
import psycopg
import uuid
import csv
import pandas as pd

#initialize database
conn = psycopg.connect(
    dbname="ooh_platform",
    host="ooh-platform.cgy3xpod6h10.ap-southeast-1.rds.amazonaws.com",
    user="ooh_rti",
    password="0^^N!pot3ncE",
    port="5432"
)

#declare the date to be used
dateY = datetime.today()
dateY = dateY - timedelta(days=1) #uncomment to fetch data from yesterday instead of today
dateY = datetime.strftime(dateY,'%Y-%m-%d')

start = time.time()
print(f'{datetime.today()}: Inserting impressions for ',dateY)
#format the dates
dateFrom = datetime.strptime(dateY + " 00:00:00" , '%Y-%m-%d  %H:%M:%S') - timedelta(hours=8)
dateTo = datetime.strptime(dateY + " 23:59:59", '%Y-%m-%d %H:%M:%S') - timedelta(hours=8)


#fetch the areas data
areas =  '/home/ubuntu/python/areas.json'
with open(areas, 'r') as f_in:
    areas = json.load(f_in)

# Read data from a CSV file using pandas
csv_file = '/home/ubuntu/python/nearest_roads_output.csv'
traffic = pd.read_csv(csv_file)

for area in areas:
    areaCode = area[:2]
    lane = {
        "C5": 4,
        "NL": 4,
        "GP": 2,
        "BG": 2,
        "ED": 4,
        "ES": 4,
        "MA": 5,
        "OR": 3,
        "QA": 6,
        "RO": 7,
        "SL": 2
    }.get(areaCode, 2)

    if areaCode == "CW":
        lane = 8 if int(area[-2:]) >= 8 or int(area[2:4]) >= 8 else 5
    new_result = predict.predict_new(area,dateFrom,dateTo, lane)
    old_result = predict.predict_old(area,dateFrom,dateTo)
    
    max_result = max(new_result,old_result)
    site_traffic = traffic[traffic['site'] == area]
    origin_impression = site_traffic['origin_impression'].values[0] if not site_traffic.empty else 0
    impression = round((origin_impression + max_result) / 3)
    
    print(f"Inserting impression for {area}: {impression} | {origin_impression}, {max_result}")
    #initialize uuid
    uuidv4 = uuid.uuid4()
    
    #run the insert query
    cur = conn.cursor()
    
    query = """INSERT INTO impressions (i_id, area, impressions, record_at) VALUES (%s, %s,%s, %s)"""
    values = (uuidv4, area,impression, dateY)
    
    cur.execute(query,values)
    
    conn.commit()
    
    cur.close()
