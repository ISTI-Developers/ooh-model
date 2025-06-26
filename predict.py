from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF
import joblib
import numpy as np
import pandas as pd
import psycopg
from datetime import datetime, timedelta 

# Connect to database
conn = psycopg.connect(
    dbname="ooh_platform",
    host="ooh-platform.cgy3xpod6h10.ap-southeast-1.rds.amazonaws.com",
    user="ooh_rti",
    password="0^^N!pot3ncE",
    port="5432"
)

def predict_new(area, dateFrom, dateTo, lane):
    # Query all data from the database
    cur = conn.cursor()
    cur.execute("""SELECT site AS t_site,
                    distance,
                    TO_CHAR(created_at,'DD/MM/YYYY') AS date,
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 4 THEN eta_normal ELSE 0 END))AS "n6",
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 10 THEN eta_normal ELSE 0 END)) AS "n12",
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 16 THEN eta_normal ELSE 0 END)) AS "n18",
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 22 THEN eta_normal ELSE 0 END)) AS "n24",
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 4 THEN eta_traffic ELSE 0 END)) AS "t6",
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 10 THEN eta_traffic ELSE 0 END)) AS "t12",
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 16 THEN eta_traffic ELSE 0 END)) AS "t18",
                    SUM((CASE WHEN EXTRACT(HOUR FROM created_at) = 22 THEN eta_traffic ELSE 0 END)) AS "t24"
                FROM travel_time WHERE site = %s
                AND created_at BETWEEN %s AND %s
                GROUP BY site, distance, date
                ORDER BY date ASC;""", [area,dateFrom, dateTo])
    
      # Check if any rows were returned
    if cur.rowcount == 0:
        print(f"{datetime.today()}: No data found for area {area}. Skipping.")
        return False
    
    dbv = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    # Convert the result into a dataframe
    x = pd.DataFrame(dbv)
    x.columns = cols

    # Convert all date strings into datetime and convert to OH timezone
    x['date'] = pd.to_datetime(x['date'],format="%d/%m/%Y")
    x["day_of_week"] = x["date"].dt.dayofweek  # Monday = 0, Sunday = 6
    x['lanes'] = lane
        
    # Move 'day_of_week' column to index 1
    cols = x.columns.tolist()
    cols.insert(1, cols.pop(cols.index('day_of_week')))
    cols.insert(4, cols.pop(cols.index('lanes')))
    x = x[cols]
    dates = x['date']
    dataX = x.select_dtypes(include=[np.number]).values.tolist()

    # Convert to NumPy array
    dataX = np.array(dataX, dtype=np.float64)


    # Normalize the input vector while avoiding division by zero
    norms = np.linalg.norm(dataX, axis=1, keepdims=True)
    dataX = np.divide(dataX, norms, where=norms != 0)


    # Load the model file
    model = joblib.load("/home/ubuntu/python/model_new.pkl")
    
    j = 0
    result = 0

    # Iterate through the input vector and predict the result per row
    for d in dataX:
        res = model.predict([d])[0]
        result = res
        j += 1
    
    return result

def predict_old(area,dateFrom,dateTo):
      # Query all data from the database
    cur = conn.cursor()
    cur.execute("""SELECT site, segment, distance, eta_normal, eta_traffic, created_at FROM travel_time WHERE site = %s AND created_at BETWEEN %s AND %s ORDER BY created_at, site, segment;""",(area, dateFrom,dateTo))
    
    # Check if any rows were returned
    if cur.rowcount == 0:
        print(f"{datetime.today()}: No data found for area {area}. Skipping.")
        return 0
    if cur.rowcount < 4:
        print(f"{datetime.today()}: Incomplete data for {area}. Skipping.")
        return 0
    
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
    return result
