### Base tables must be created first 
#    Payers -- data loaded
#    Organization -- data loaded
#    Provider -- data loaded
#    Patient -- data loaded
## Expanded tables created next 
#    Encounters -- data loaded
#    Conditions -- data loaded
#    Procedures -- data loaded
#    Observations -- data loaded
#    Careplans -- data loaded
## Final table to be created
#    Claims -- data loaded

import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
import sys

### Populate tables by loading CSV Files ###

csv_data = pd.read_csv("SyntheaData/csv/providers.csv", index_col = False , delimiter = ',')
csv_data.head()

try:#give ur username, password
    conn = msql.connect(host='host',database = 'synlaunch', user='*******',password='*******')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_data.iterrows():
        sql = "INSERT IGNORE INTO Provider(ID, NAME, GENDER, SPECIALTY, ADDRESS, CITY, \
        STATE, ZIP, LAT, LON, UTILIZATION , ORGANIZATION_ID,  PROCEDURES)\
        VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)"
        #print("Record Inserted")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)


#### LOADING PATIENT DATA

csv_dat2 = pd.read_csv("SyntheaData/csv/patientsV2.csv", index_col = False , delimiter = ',')
csv_dat2 = csv_dat2.fillna(0)
#csv_data.head()

try:#give ur username, password
    conn = msql.connect(host='host',database = 'synlaunch',user='*******',password='*******')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_dat2.iterrows():
        sql = "INSERT IGNORE INTO Patients(ID, BIRTHDATE, DEATHDATE, SSN, DRIVERS, PASSPORT, PREFIX, FIRST, LAST, SUFFIX, MAIDEN, MARITAL, RACE, ETHNICITY, GENDER, BIRTHPLACE, ADDRESS, CITY, STATE, COUNTY, FIPS, ZIP, LAT, LON, HEALTHCARE_EXPENSES, HEALTHCARE_COVERAGE, INCOME)\
        VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #print("Record Added")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)



### Loading Encounters Data

csv_dat3 = pd.read_csv("SyntheaData/csv/encounters.csv", index_col = False , delimiter = ',')
csv_dat3 = csv_dat3.fillna(0)
#csv_data.head()

try:#give ur username, password
    conn = msql.connect(host='host',database = 'synlaunch', user='*******',password='*******')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_dat3.iterrows():
        sql = "INSERT IGNORE INTO Encounters(ID, START, STOP,PATIENT_ID, ORGANIZATION_ID,PROVIDER_ID, PAYER_ID,ENCOUNTERCLASS,CODE,DESCRIPTION,BASE_ENCOUNTER_COST,TOTAL_CLAIM_COST,PAYER_COVERAGE,REASONCODE,REASONDESCRIPTION)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #print("Record Added")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)


### Loading Conditions Data

csv_dat4 = pd.read_csv("SyntheaData/csv/conditionsV2.csv", index_col = False , delimiter = ',')
csv_dat4 = csv_dat4.fillna(0)
#csv_data.head()

try:#give ur username, password
    conn = msql.connect(host='host',database = 'synlaunch', user='*******',password='*******')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_dat4.iterrows():
        sql = "INSERT IGNORE INTO Conditions(PK_ID, START, STOP, PATIENT_ID, ENCOUNTER, CODE, DESCRIPTION)\
        VALUES (%s,%s,%s,%s,%s,%s,%s)"
        #print("Record Added")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)

### Loading Procedures Data

csv_dat6 = pd.read_csv("SyntheaData/csv/proceduresV2.csv", index_col = False , delimiter = ',')
csv_dat6 = csv_dat6.fillna(0)
#csv_data.head()

try:#give ur username, password
    conn = msql.connect(host='host',database = 'synlaunch', user='*******',password='*******')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_dat6.iterrows():
        sql = "INSERT IGNORE INTO Procedures(PK_ID, START,STOP,PATIENT_ID, ENCOUNTER, CODE, DESCRIPTION, BASE_COST, REASONCODE, REASONDESCRIPTION)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #print("Record Added")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)


## Loading Careplans Data

csv_dat7 = pd.read_csv("SyntheaData/csv/careplansV2.csv", index_col = False , delimiter = ',')
csv_dat7 = csv_dat7.fillna(0)
#csv_data.head()

try:#give ur username, password
    conn = msql.connect(host='host',database = 'synlaunch', user='*******',password='*******')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_dat7.iterrows():
        sql = "INSERT IGNORE INTO Careplan(Id,START, STOP, PATIENT_ID, ENCOUNTER, CODE, DESCRIPTION ,REASONCODE, REASONDESCRIPTION)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #print("Record Added")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)

### Loading Observations Data
### note this was done through multiple files running on separate kernels, using different users to load data in parallel

csv_dat8a = pd.read_csv("SyntheaData/csv/ObservationBreakdown/filtered_observations3V2-110kd.csv", index_col = False , delimiter = ',')
csv_dat8a = csv_dat8a.fillna(0)
#csv_data.head()

try:#give ur username, password
    conn = msql.connect(host='databasesynthea.c3hnw2e5ik8r.us-east-1.rds.amazonaws.com',database = 'synlaunch', user='*******',password='*******')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_dat8a.iterrows():
        sql = "INSERT IGNORE INTO Observations(PK_ID, DATE, PATIENT_ID,ENCOUNTER,CATEGORY,CODE,DESCRIPTION,VALUE,UNITS,TYPE)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #print("Record Added")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)



#### 
#### File hunt in existing directory
import glob
import pandas as pd
  
# specifying the path to csv files
path = "SyntheaData/csv/ObservationBreakdown"
  
# csv files in the path
files = glob.glob(path + "/*.csv")
  
# defining an empty list to store 
# content
data_frame = pd.DataFrame()
content = []
  
# checking all the csv files in the 
# specified path
for filename in files:
    print(filename)
    
    # reading content of csv file
    # content.append(filename)

  

