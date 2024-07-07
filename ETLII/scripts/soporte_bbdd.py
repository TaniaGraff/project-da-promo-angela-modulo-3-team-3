#Importamos la librería pandas que necesitamos para la lectura, conversión y limpieza de los datos
# -----------------------------------------------------------------------
import pandas as pd
import numpy as np
import re 

#Importamos librerías necesarias para la visualización
# -----------------------------------------------------------------------
import matplotlib.pyplot as plt
import seaborn as sns

#Importamos librerías para localizar ruta archivos.
import os
import sys
sys.path.append("../")

# Imputación de nulos usando métodos avanzados estadísticos
# -----------------------------------------------------------------------
from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import KNNImputer

# Importamos librería para la conexión con MySQL
# -----------------------------------------------------------------------
import mysql.connector
from mysql.connector import errorcode

pd.set_option('display.max_columns', None) # para poder visualizar todas las columnas de los DataFrames
pd.set_option('display.max_rows', None) # para poder visualizar todas las filas de los DataFrames

"""Creamos función para abrir los csv y convertirlos en DF.
"""
def lectura_archivo(csv):
    df = pd.read_csv(csv)
    return df

"""Creamos función para resetear el índice de los DF y renombrarlo.
"""
def resetear_indice_y_renombrar(df):
    df_reset = df.reset_index()
    df_reset.rename(columns={'index': 'ID_FK'}, inplace=True)
    return df_reset


"""Creamos función para guardar los DF resultantes en la carpeta output_data de la FASE 2.
"""
def guardar_df(df, nombre_archivo, output_folder='/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETLII/data/output_data/'):
    ruta_archivo = os.path.join(os.path.dirname(__file__), '..', output_folder, nombre_archivo + '.csv')
    df.to_csv(ruta_archivo, index=False)
    print(f"El DataFrame se ha guardado en {ruta_archivo}")

"""Creamos función para crear BBDD
"""
def crear_base_de_datos(user, password, host='127.0.0.1'):
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host)
        mycursor = cnx.cursor()

        mycursor.execute("CREATE DATABASE IF NOT EXISTS ABC_Corporation")

        print("Base de datos 'ABC_Corporation' creada correctamente.")

    except mysql.connector.Error as err:
        print(f"Error al crear la base de datos: {err}")
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    finally:
        if 'cnx' in locals() and cnx.is_connected():
            mycursor.close()
            cnx.close()

"""Creamos función para crear tabla empleados.
"""
def crear_tabla_empleados(user, password, host='127.0.0.1', database='ABC_Corporation'):
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
        mycursor = cnx.cursor()

        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Employees (
            ID_FK INT AUTO_INCREMENT PRIMARY KEY,
            Age INT,
            Gender VARCHAR(100),
            MaritalStatus VARCHAR(100),
            DateBirth YEAR,
            Education INT,
            EducationField VARCHAR(100),
            Attrition VARCHAR(100),
            BusinessTravel VARCHAR(100),
            DistanceFromHome INT
            )
        """)
        print("Tabla 'Employees' creada correctamente.")

        cnx.close()
    except mysql.connector.Error as err:
        print(f"Error al crear la tabla Employees: {err}")
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    finally:
        if 'cnx' in locals() and cnx.is_connected():
            mycursor.close()
            cnx.close()


"""Creamos función para crear tabla job_details.
"""
def crear_tabla_job_details(user, password, host='127.0.0.1', database='ABC_Corporation'):
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
        mycursor = cnx.cursor()

        mycursor.execute("""
            CREATE TABLE JobDetails (
            ID_FK INT AUTO_INCREMENT PRIMARY KEY,
            JobLevel INT,
            JobRole VARCHAR(250),
            NumCompaniesWorked INT,
            TotalWorkingYears VARCHAR(100),
            YearsAtCompany INT,
            YearsSinceLastPromotion INT,
            YearsWithCurrManager INT
            )
        """)
        print("Tabla 'JobDetails' creada correctamente.")

    except mysql.connector.Error as err:
        print(f"Error al crear la tabla JobDetails: {err}")
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    finally:
        if 'cnx' in locals() and cnx.is_connected():
            mycursor.close()
            cnx.close()


"""Creamos función para crear tabla compensation.
"""
def crear_tabla_compensation(user, password, host='127.0.0.1', database='ABC_Corporation'):
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
        mycursor = cnx.cursor()

        mycursor.execute("""
            CREATE TABLE Compensation (
            ID_FK INT AUTO_INCREMENT PRIMARY KEY,
            DailyRate INT,
            HourlyRate INT,
            MonthlyIncome INT,
            MonthlyRate INT,
            OverTime VARCHAR(100),
            PercentSalaryHike INT,
            StockOptionLevel INT,
            TrainingTimesLastYear INT,
            RemoteWork VARCHAR(100)
            )
        """)
        print("Tabla 'Compensation' creada correctamente.")

    except mysql.connector.Error as err:
        print(f"Error al crear la tabla Compensation: {err}")
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    finally:
        if 'cnx' in locals() and cnx.is_connected():
            mycursor.close()
            cnx.close()


"""Creamos función para crear tabla satisfaction.
"""
def crear_tabla_satisfaction(user, password, host='127.0.0.1', database='ABC_Corporation'):
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
        mycursor = cnx.cursor()

        mycursor.execute("""
            CREATE TABLE Satisfaction (
            ID_FK INT AUTO_INCREMENT PRIMARY KEY,
            EnvironmentSatisfaction INT,
            JobInvolvement INT,
            JobSatisfaction INT,
            PerformanceRating VARCHAR(100),
            RelationshipSatisfaction INT,
            WorkLifeBalance INT
            )
        """)
        print("Tabla 'Satisfaction' creada correctamente.")


    except mysql.connector.Error as err:
        print(f"Error al crear la tabla Satisfaction: {err}")
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    finally:
        if 'cnx' in locals() and cnx.is_connected():
            mycursor.close()
            cnx.close()

""" ____________________________________           INSERCIÓN DATOS            _____________________________________"""

"""Creamos función para insertar datos en las tablas de la BBDD ABC_CORPORATION
"""
import soporte_bbdd

def leer_csv_con_soporte(file_path):
    df = soporte_bbdd.lectura_archivo(file_path)
    lista_tuplas = [tuple(x) for x in df.to_numpy()]
    return lista_tuplas



def insertar_datos(query, user, password, host='127.0.0.1', database='ABC_Corporation', lista_tuplas=None, file_path=None):
    if file_path:
        lista_tuplas = leer_csv_con_soporte(file_path)

    if lista_tuplas:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
        mycursor = cnx.cursor()
        
        try:
            mycursor.executemany(query, lista_tuplas)
            cnx.commit()
            print(mycursor.rowcount, 'registros insertados')
            cnx.close()

        except mysql.connector.Error as err:
            print(f"Error al insertar los datos: {err}")
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
    else:
        print("No hay datos para insertar")
    
