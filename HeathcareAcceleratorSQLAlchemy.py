#!/usr/bin/env python
# coding: utf-8
#===================================================================================
# This is the connecting to and manipulating a mySQL database using sqlAlchemy
#===================================================================================
# Versions:
# Who            When        Why
# ---------      ----------- -------------------------------------------------------
# Rob Cardona    2/27/2023   connecting to mySQL for reasons
#                            pip install sqlalchemy
#                            pip install mysql-connector
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
#===================================================================================
# TODO: 
#===================================================================================

import os
import sys
import configparser
import mysql.connector 
import sqlalchemy
import uuid
import pandas as pd

from mysql.connector import connection
from datetime import datetime as dt, date 
from sqlalchemy import * 
from sqlalchemy.orm import  sessionmaker, Session, declarative_base
from sqlalchemy.engine import reflection
from sqlalchemy.orm import registry, relationship, sessionmaker, load_only, Load
from mysql.connector import Error

## Read config file to pull in credentials
config = configparser.ConfigParser()
config.read('config.ini')

dbhost = config['dbcred']['endpoint']
dbuser = config['dbcred']['user']
dbpw = config['dbcred']['password']
dbport = config['dbcred']['port']

## Create connection string using mysqldb dialect        
conn = f"mysql+mysqldb://{dbuser}:{dbpw}@{dbhost}:{dbport}/synlaunch"
engine = create_engine(conn, pool_pre_ping=True, future = True)
mapper_registry = registry()

#Verifying engine connection to the DB by confirming existence and return of the dedicated user#
#----------------------------------------------------------------------------------------------#
connection = engine.connect()

stmt = text("Select user from mysql.user where user =:user")
result = connection.execute(stmt,{"user": dbuser})
row = result.first()

if row.user == dbuser:
    print("Connection successfull")
else:
    print('Connection failed')

connection.close()

## Running a query from SQL Alchemy 
connection = engine.connect()
stmt = text("Select * FROM synlaunch.Patients")
result = connection.execute(stmt)
row = result.first()
print(row)

## Alternate result
result2 = connection.execute(stmt)
result.all()

## Use this for clearing all existing Mappers 
## This is usefull for when changes have been made from the original iteration of the table

sqlalchemy.orm.clear_mappers()

#### This defines the uuid allowing a string to be passed as a primary key usually a BAD IDEA , exception was made in this instance

def generate_uuid():
    return str(uuid.uuid4())

##### SQL TABLE DEFINITIONS ######
##### note that mySQL requires the string datatype have a declared size
##### LOAD SHOULD BE BROKEN UP INTO 3 SECTIONS to rrevent primary/foreign key constraint issues 
################################################################################################################################
##LOAD 1
################################################################################################################################
@mapper_registry.mapped
class Payers():
    __tablename__ = "Payers"
    ID = Column(String(36), primary_key= True, default=generate_uuid, nullable = False)
    NAME= Column(String(70), nullable = False)
    OWNERSHIP = Column(String(70))
    ADDRESS = Column(String(70))
    CITY = Column(String(70))
    STATE_HEADQUARTERED = Column(String(70))
    ZIP= Column(String (10))
    PHONE = Column(String(28))
    AMOUNT_COVERED= Column(Float (10,2), nullable = False)
    AMOUNT_UNCOVERED= Column (Float (10,2), nullable = False)
    REVENUE= Column (Float (10,2) , nullable = False)
    COVERED_ENCOUNTERS = Column(Integer, nullable = False)
    UNCOVERED_ENCOUNTERS= Column(Integer, nullable = False)
    COVERED_MEDICATIONS= Column(Integer,  nullable = False)
    UNCOVERED_MEDICATIONS= Column(Integer,  nullable = False)
    COVERED_PROCEDURES= Column(Integer, nullable = False)
    UNCOVERED_PROCEDURES= Column(Integer, nullable = False)
    COVERED_IMMUNIZATIONS= Column(Integer, nullable = False)
    UNCOVERED_IMMUNIZATIONS= Column(Integer, nullable = False)
    UNIQUE_CUSTOMERS= Column(Integer, nullable = False)
    QOLS_AVG= Column(Integer, nullable = False)
    MEMBER_MONTHS= Column(Integer,nullable = False)
    __table_args__ = {'extend_existing': True}

    encounters = relationship("Encounters", back_populates = 'payers')
    claims = relationship('Claims', back_populates = 'payers')

@mapper_registry.mapped
class Provider():
    __tablename__ = 'Provider'
    ID = Column(String(36), primary_key= True, default=generate_uuid, nullable = False)
    NAME =  Column(String(30),nullable = False)
    GENDER = Column(String(1),nullable = False)
    SPECIALTY = Column(String (46),nullable = False)
    ADDRESS = Column(String (50),nullable = False)
    CITY = Column(String(16),nullable = False)
    STATE = Column(String(2))
    ZIP= Column(String(10))
    LAT = Column(Float(10,8))
    LON = Column(Float(11,8))
    UTILIZATION = Column(Integer,nullable = False)
    ORGANIZATION_ID = Column(ForeignKey("Organization.ID"),nullable = False)
    PROCEDURES = Column(Integer)
    __table_args__ = {'extend_existing': True}

    organization = relationship("Organization", back_populates = 'provider')
    encounters = relationship("Encounters", back_populates = 'provider')
    claims = relationship("Claims", back_populates = 'provider')
    
@mapper_registry.mapped
class Patients():
    __tablename__ = "Patients"
    ID = Column(String(36), primary_key= True, default=generate_uuid, nullable = False)
    BIRTHDATE = Column(DATE, nullable = False)
    DEATHDATE= Column(DATE)
    SSN= Column(String(11) , nullable = False)
    DRIVERS= Column(String(9))
    PASSPORT= Column(String(10))
    PREFIX= Column(String(4))
    FIRST= Column(String(18) , nullable = False)
    LAST= Column(String(16) , nullable = False)
    SUFFIX= Column(String(3))
    MAIDEN= Column(String(16))
    MARITAL= Column(String(1))
    RACE= Column(String (8) , nullable = False)
    ETHNICITY= Column(String(11) , nullable = False)
    GENDER= Column(String(1) , nullable = False)
    BIRTHPLACE= Column(String(80) , nullable = False)
    ADDRESS= Column(String(35) , nullable = False)
    CITY= Column(String(33) , nullable = False)
    STATE= Column(String(10) , nullable = False)
    COUNTY= Column(String(22)) 
    FIPS= Column(INTEGER)
    ZIP= Column(String(10))
    Lat = Column(Float(10,8))
    Lon = Column(Float(11,8))
    HEALTHCARE_EXPENSES= Column(FLOAT(10,2), nullable = False)
    HEALTHCARE_COVERAGE= Column(FLOAT(10,2), nullable = False)
    INCOME = Column(Integer, nullable = False)
    __table_args__ = {'extend_existing': True}
    
    claims = relationship ("Claims", back_populates = 'patients')
    encounters = relationship("Encounters", back_populates = 'patients')
    conditions = relationship("Conditions", back_populates = 'patients')
    procedures = relationship("Procedures", back_populates = 'patients')
    observations = relationship("Observations", back_populates = 'patients')
    careplans = relationship ("Careplans", back_populates = 'patients')
                              
                                 
@mapper_registry.mapped
class Organization():
    __tablename__ = 'Organization'
    ID = Column(String(36), default=generate_uuid, nullable = False, primary_key = True)  
    NAME = Column(String(70), nullable = False)                                           
    ADDRESS = Column(String(70), nullable = False)                                        
    CITY = Column(String(25), nullable = False)                                           
    STATE = Column(String (2))                                                            
    ZIP = Column(String(10))                                                              
    LAT = Column(Float(10,8))                                                          
    LON = Column(Float (11,8))                                                         
    PHONE = Column(String(28))                                                         
    REVENUE = Column(Float(16,2), nullable = False)                                  
    UTILIZATION = Column(Integer, nullable = False)
    __table_args__ = {'extend_existing': True}

    provider = relationship("Provider", back_populates = 'organization')
    encounters = relationship('Encounters', back_populates = 'organization')
    

#################################################################################################
##LOAD 2
#################################################################################################

def generate_uuid():
    return str(uuid.uuid4())


    
@mapper_registry.mapped
class Encounters():
    __tablename__ = 'Encounters'
    ID = Column(String(36), primary_key= True, default=generate_uuid, nullable = False)
    START = Column(DateTime, nullable = False)
    STOP = Column(DateTime)
    PATIENT_ID = Column(ForeignKey('Patients.ID'), nullable = False)
    ORGANIZATION_ID= Column(ForeignKey('Organization.ID'), nullable = False)
    PROVIDER_ID= Column(ForeignKey('Provider.ID'), nullable = False)
    PAYER_ID= Column(ForeignKey('Payers.ID'), nullable = False)
    ENCOUNTERCLASS = Column(String(15),nullable = False)
    CODE = Column(String(15), nullable = False)
    DESCRIPTION = Column(String(70) , nullable = False)
    BASE_ENCOUNTER_COST = Column(Float(16,2), nullable = False) 
    TOTAL_CLAIM_COST = Column(Float(16,2), nullable = False) 
    PAYER_COVERAGE = Column(Float(16,2), nullable = False) 
    REASONCODE = Column(String(20))
    REASONDESCRIPTION = Column(String(70))
    __table_args__ = {'extend_existing': True}
                         
    patient = relationship("Patients", back_populates = 'encounters')
    organization = relationship("Organization", back_populates = 'encounters')
    provider = relationship("Provider", back_populates = 'encounters')
    payers = relationship("Payers", back_populates = 'encounters')
    claims = relationship("Claims", back_populates = 'encounters')     
    conditions = relationship("Conditions", back_populates = 'encounters')
                         
@mapper_registry.mapped
class Conditions():
    __tablename__ = 'Conditions'
    PK_ID = Column(Integer, primary_key=True)
    START = Column(Date, nullable = False)
    STOP = Column(Date)
    PATIENT_ID = Column(ForeignKey('Patients.ID'), nullable = False)
    ENCOUNTER  = Column(ForeignKey('Encounters.ID'),nullable = False)
    CODE = Column(String(20), nullable = False)
    DESCRIPTION = Column(String(90), nullable = False)
    __table_args__ = {'extend_existing': True}
    
    patient = relationship("Patients", back_populates = 'conditions')
    encounters = relationship ("Encounters", back_populates = 'conditions')
                                              
                    
                         
@mapper_registry.mapped
class Procedures():
    __tablename__ = 'Procedures'
    PK_ID = Column(Integer, primary_key=True)
    START = Column(DateTime, nullable = False)
    STOP = Column(DateTime)
    PATIENT_ID = Column(ForeignKey('Patients.ID'), nullable = False)
    ENCOUNTER  = Column(ForeignKey('Encounters.ID'),nullable = False)
    CODE = Column(String (20), nullable = False)
    DESCRIPTION = Column(String(150),nullable = False)
    BASE_COST = Column(Float(16,2), nullable = False)
    REASONCODE = Column (String(20))
    REASONDESCRIPTION = Column(String(70))
    __table_args__ = {'extend_existing': True}
                         
    patient = relationship("Patients", back_populates = 'procedures')
    encounters = relationship ("Encounters", back_populates = 'procedures')
                         
@mapper_registry.mapped
class Observations():
    __tablename__ = 'Observations'
    PK_ID = Column(Integer, primary_key=True)
    DATE = Column(DateTime, nullable = False)
    PATIENT_ID = Column(ForeignKey('Patients.ID'), nullable = False)
    ENCOUNTER  = Column(ForeignKey('Encounters.ID'),nullable = False)
    CATEGORY = Column(String(14))
    CODE= Column(String(8),nullable = False)
    DESCRIPTION = Column(String(250),nullable = False)
    VALUE = Column(String(120),nullable = False)
    UNITS = Column(String(16))
    TYPE = Column(String(7),nullable = False)
    __table_args__ = {'extend_existing': True}
                         
    patient = relationship("Patients", back_populates = 'observations')
    encounters = relationship ("Encounters", back_populates = 'observations')
                                                  
@mapper_registry.mapped
class Careplan():
    __tablename__ = 'Careplan'
    ID = Column(String(36), primary_key= True, default=generate_uuid, nullable = False)
    START = Column(Date, nullable = False)
    STOP = Column(Date)
    PATIENT_ID = Column(ForeignKey('Patients.ID'), nullable = False)
    ENCOUNTER  = Column(ForeignKey('Encounters.ID'),nullable = False)
    CODE = Column(String (9), nullable = False)
    DESCRIPTION= Column(String (62), nullable = False)
    REASONCODE= Column(String (14), nullable = False)
    REASONDESCRIPTION= Column(String (69), nullable = False)
    __table_args__ = {'extend_existing': True}

    patient = relationship("Patients", back_populates = 'careplan')
    encounters = relationship ("Encounters", back_populates = 'careplan')
                         
#################################################################################################
##LOAD 3
#################################################################################################


def generate_uuid():
    return str(uuid.uuid4())

@mapper_registry.mapped
class Claims():            
    __tablename__ = 'Claims'
    ID = Column(String(36), primary_key= True, default=generate_uuid, nullable = False)
    PATIENT_ID = Column(ForeignKey('Patients.ID'), nullable = False)
    PROVIDER_ID = Column(ForeignKey('Provider.ID'), nullable = False)
    PRIMARYPATIENTINSURANCEID = Column(ForeignKey('Payers.ID'))
    SECONDARYPATIENTINSURANCEID = Column(String(36))
    DEPARTMENTID = Column(Integer)
    PATIENTDEPARTMENTID= Column(Integer)
    DIAGNOSIS1= Column(String(19))
    DIAGNOSIS2= Column(String(19))
    DIAGNOSIS3= Column(String(19))
    DIAGNOSIS4= Column(String(19))
    DIAGNOSIS5= Column(String(19))
    DIAGNOSIS6= Column(String(19))
    DIAGNOSIS7= Column(String(19))
    DIAGNOSIS8= Column(String(19))
    REFERRINGPROVIDERID = Column(ForeignKey('Provider.ID'))
    APPOINTMENTID =  Column(String(36))
    CURRENTILLNESSDATE = Column(DateTime, nullable = False)
    SERVICEDATE  = Column(DateTime, nullable = False)
    SUPERVISINGPROVIDERID =  Column(String(36))
    STATUS1 = Column(String(8))
    STATUS2 = Column(String(8))
    STATUSP = Column(String(8))
    OUTSTANDING1 = Column(String(8))
    OUTSTANDING2 = Column(String(8))
    OUTSTANDINGP = Column(String(8))
    LASTBILLEDDATE1 = Column(DateTime)
    LASTBILLEDDATE2 = Column(DateTime)
    LASTBILLEDDATEP = Column(DateTime)
    HEALTHCARECLAIMTYPEID1 = Column(Integer)
    HEALTHCARECLAIMTYPEID2  = Column(Integer)
    __table_args__ = {'extend_existing': True}
    
    patient = relationship("Patients", back_populates = 'claims')
    encounters = relationship ("Encounters", back_populates = 'claims')
    payers = relationship("Payers", back_populates = 'claims')
    provider = relationship("Providers", back_populates = 'claims')
                         
    
## Commiting all classes to the MYSQL Database 
with engine.begin() as con:
    mapper_registry.metadata.create_all(con)


   