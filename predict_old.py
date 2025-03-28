import joblib
import numpy as np
import pandas as pd
import psycopg
from datetime import datetime, timedelta 
import json
import uuid
import time


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
with open(areas,'r') as f_in:
    areas = json.load(f_in)

#run the sql query for each area to generate impressions
for area in areas:
    # Query all data from the database
    cur = conn.cursor()
    cur.execute("""SELECT site, segment, distance, eta_normal, eta_traffic, created_at FROM travel_time WHERE site = %s AND created_at BETWEEN %s AND %s ORDER BY created_at, site, segment;""",(area, dateFrom,dateTo))
    
    # Check if any rows were returned
    if cur.rowcount == 0:
        print(f"{datetime.today()}: No data found for area {area}. Skipping.")
        continue
    if cur.rowcount < 4:
        print(f"{datetime.today()}: Incomplete data for {area}. Skipping.")
        continue
        
    dbv = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    # Convert the result into a dataframe
    f = pd.DataFrame(dbv)
    f.columns = cols

    f['created_at'] = pd.to_datetime(f['created_at'])
    f['created_at'] = f['created_at'].dt.tz_localize('UTC') 
    f['created_at'] = f['created_at'].dt.tz_convert('Asia/Manila')
    f['weekend'] = np.where(f['created_at'].dt.day_of_week > 4, 1, 0)

    f['date'] = f['created_at'].dt.date
    f['hour'] = f['created_at'].dt.hour

    pIdx = ['site', 'date', 'segment', 'weekend']
    f = f.pivot(index = pIdx, columns = 'hour', values = ['eta_normal', 'eta_traffic'])
    f = f.droplevel(2)

    f.reset_index(inplace = True)
    f.bfill(inplace = True)

    columns = ['site', 'new_date', 'weekend',
            '0_n', '6_n', '12_n', '18_n',
            '0_t', '6_t', '12_t', '18_t']
    f = pd.DataFrame(f.values, columns = columns)
    dataX = f.iloc[:,2:]
    dataX = dataX.values
    dataX = np.array(dataX)

    model = joblib.load("/home/ubuntu/python/MODEL_3.pkl")
    
    # Iterate through the input vector and predict the result per row
    [result] = model.predict(dataX)
    
    #initialize uuid
    uuidv4 = uuid.uuid4()
    
    #run the insert query
    cur = conn.cursor()
    
    query = """INSERT INTO impressions (i_id, area, impressions, record_at) VALUES (%s, %s,%s, %s)"""
    values = (uuidv4, f['site'][0], result, dateY)
    
    cur.execute(query,values)
    
    conn.commit()
    
    cur.close()
    print(f'{datetime.today()}: Success inserting area {f["site"][0]}')
    
end = time.time()

elap = end - start
elap = time.strftime("%H:%M:%S", time.gmtime(elap))
print(f'{datetime.today()}: Success inserting all {len(areas)} impressions for {dateY}')
print(f'{datetime.today()}: Time Elapsed: {elap}')
conn.close()
