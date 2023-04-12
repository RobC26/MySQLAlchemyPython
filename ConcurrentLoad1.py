#!/usr/bin/env python
# coding: utf-8


import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
import sys

## Read data and csv file info to determine fields 
csv_dat1 = pd.read_csv("SyntheaData/csv/claimsV3.csv", index_col = False , delimiter = ',', dtype = 'unicode')
csv_dat1 = csv_dat1.fillna(0) # Fill in null values with 0
csv_dat1.head()
#csv_dat8a.info()

## Peform actual import
csv_dat1 = pd.read_csv("SyntheaData/csv/claimsV3.csv", index_col=False, delimiter = ',', dtype = 'unicode')
csv_dat1 = csv_dat1.fillna(0)


try:#give ur username, password
    conn = msql.connect(host='databasesynthea.c3hnw2e5ik8r.us-east-1.rds.amazonaws.com',database = 'synlaunch', user='User',password='Password')
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database : ", record)
# Loop through the dataframe if conn.is_connected():
    for i, row in csv_dat1.iterrows():
        sql = "INSERT IGNORE INTO dev_Claims(ID,PATIENT_ID,PROVIDER_ID,PRIMARYPATIENTINSURANCEID,SECONDARYPATIENTINSURANCEID,DEPARTMENTID,PATIENTDEPARTMENTID,DIAGNOSIS1,DIAGNOSIS2,DIAGNOSIS3,DIAGNOSIS4,DIAGNOSIS5,DIAGNOSIS6,DIAGNOSIS7,DIAGNOSIS8,REFERRINGPROVIDERID,APPOINTMENTID,CURRENTILLNESSDATE,SERVICEDATE,SUPERVISINGPROVIDERID,STATUS1,STATUS2,STATUSP,OUTSTANDING1,OUTSTANDING2,OUTSTANDINGP,LASTBILLEDDATE1,LASTBILLEDDATE2,LASTBILLEDDATEP,HEALTHCARECLAIMTYPEID1,HEALTHCARECLAIMTYPEID2)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #print("Record Added")
        cursor.execute(sql, tuple(row))
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)

