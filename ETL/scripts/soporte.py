#Importamos librerías
import pandas as pd
import numpy as np
import re

import os
import sys
sys.path.append("../")

import warnings
warnings.filterwarnings("ignore")

from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import KNNImputer


"""Creamos función para abrir el csv y convertir en DF.
"""
def lectura_archivo(csv):
    df = pd.read_csv(csv, index_col= 0)
    return df

"""Creamos función para explorar los datos"""
def exploracion_datos(df):
    print('_____________ INFORMACIÓN GENERAL DEL DATAFRAME ____________\n')
    print(df.info())

    print('___________________ FORMA DEL DATAFRAME ____________________\n')
    print(f"El número de filas que tenemos es de {df.shape[0]}.\nEl número de columnas es de {df.shape[1]}\n")

    print('_______________ NULOS, ÚNICOS Y DUPLICADOS _________________\n')
    
    print('La cantidad de valores NULOS por columna es de:\n')
    print(df.isnull().sum())
    print('____________________________________________________________\n')

    print('La cantidad de valores ÚNICOS por columna es de:\n')
    for columna in df.columns:
        cantidad_valores_unicos = df[columna].nunique()
        print(f'La columna {columna}: {cantidad_valores_unicos}')
    print('____________________________________________________________\n')

    print('La cantidad de valores DUPLICADOS por columna es de:\n')
    for columna in df.columns:
        cantidad_duplicados = df[columna].duplicated().sum()
        print(f'La columna {columna}: {cantidad_duplicados}')

    print('____________________ RESUMEN ESTADÍSTICO ____________________')
    print('____________________ Variables Numéricas __________________\n')
    print(df.describe().T)
    
    print('___________________ Variables Categóricas _________________\n')
    print(df.describe(include='object').T)


"""Creamos función para eliminar columnas
"""
lista_columnas_eliminar = ['Over18', 'StandardHours', 'RoleDepartament', 'Department', 'YearsInCurrentRole', 'NUMBERCHILDREN', 'employeecount', 'SameAsMonthlyIncome', 'Salary']

def eliminar_columnas (df):
    df.drop(columns=lista_columnas_eliminar, inplace=True)


"""Creamos función para reemplazar valores columna Gender
"""
def reemplazar_valores_genero(df, columna='Gender'):
    df.loc[df[columna] == 0, columna] = 'M'
    df.loc[df[columna] == 1, columna] = 'F'

"""Creamos función para reemplazar valores columna RemoteWork
"""
def remplazar_valores_remotework(df):
    def map_values(value):
        if str(value).lower() in ['1', 'true', 'yes']:
            return 'yes'
        elif str(value).lower() in ['0', 'false', 'no']:
            return 'no'
        else:
            return value

    df['RemoteWork'] = df['RemoteWork'].apply(map_values)
    return df

"""Creamos función para cambiar columnas a tipo float.
"""
lista_tofloat=['DailyRate', 'HourlyRate', 'MonthlyIncome']

def limpiar_y_convertir_a_float(df):
    def cambiar_comas(cadena):
        try:
            cleaned_string = re.sub(r'[^\d,]', '', str(cadena))  
            cleaned_string = cleaned_string.replace(",", ".")
            return float(cleaned_string)
        except (ValueError, TypeError):
            return np.nan

    for col in lista_tofloat:
        df[col] = pd.to_numeric(df[col].apply(cambiar_comas), errors='coerce')

"""Creamos función para cambiar columnas a interger
"""
def limpiar_y_convertir_a_entero(df):
 
    def comasapuntos(num):
        try:
            return float(num.replace(",", "."))
        except (ValueError, AttributeError):
            return np.nan

    df['WORKLIFEBALANCE'] = df['WORKLIFEBALANCE'].apply(comasapuntos)
    df['WORKLIFEBALANCE'] = pd.to_numeric(df['WORKLIFEBALANCE'], errors='coerce').astype('Int64')

 
"""Creamos función para cambiar columna Age a interger
"""
def corregir_edades(df):
    correciones = {
        'forty-seven': '47',
        'fifty-eight': '58',
        'thirty-six': '36',
        'fifty-five': '55',
        'fifty-two': '52',
        'thirty-one': '31',
        'thirty': '30',
        'twenty-six': '26',
        'thirty-seven': '37',
        'thirty-two': '32',
        'twenty-four': '24'
    }
    df['Age'] = df['Age'].replace(correciones)

    df['Age'] = pd.to_numeric(df['Age'], errors='coerce').astype('Int64')

"""Creamos función para cambiar valores negativos columna DistanceFromHome
"""
def sustituir_negativos(df, columna='DistanceFromHome'):
    df.loc[df[columna] < 0, columna] = df[columna].abs()
    return df

"""Creamos función para corregir errores tipográficos columna MaritalStatus
"""
def corregir_errores_maritalstatus(df):
    df.loc[df['MaritalStatus'] == 'Marreid', 'MaritalStatus'] = 'Married'
    df.loc[df['MaritalStatus'] == 'divorced', 'MaritalStatus'] = 'Divorced'

"""Creamos función para igualar mayúsculas y minúsculas
"""
def normalizar_y_renombrar(df):
    
    df['JobRole'] = df['JobRole'].str.title()

    renombrar_columnas = {
        'TOTALWORKINGYEARS': 'TotalWorkingYears',
        'employeenumber': 'EmployeeNumber',
        'NUMCOMPANIESWORKED': 'NumCompaniesWorked',
        'WORKLIFEBALANCE': 'WorkLifeBalance',
        'YEARSWITHCURRMANAGER': 'YearsWithCurrManager'
    }
    df.rename(columns=renombrar_columnas, inplace=True)

"""Creamos función para contar las frecuencias de cada columna
"""
def contar_frequencias(df):
    contador_frecuencias = {}
    
    for column in df.columns:
        contador_frecuencias[column] = df[column].value_counts()
        
        print(f"Conteo de frecuencias para la columna {column}:")
        print(contador_frecuencias[column])
        print("\n") 

"""Creamos función para eliminar duplicados columna EmployeeNumber
"""
def eliminar_duplicados(df):
    df.drop_duplicates(subset=['EmployeeNumber'], inplace=True)


"""Creamos función para obtener los nulos del DF.
"""
def nulos(df):
    for columna in df.columns:
        num_filas_nulas = df[columna].isnull().sum()
        print(f'{columna.upper()}: {num_filas_nulas}')

"""Creamos función para ver porcentaje de nulos categóricos
"""
def nulos_categoricos(df):
    nulos_cat = df[df.columns[df.isnull().any()]].select_dtypes(include="object").columns.tolist()
    return nulos_cat

"""Creamos función para ver cómo se distribuyen los valores en esas columnas con nulos categóricos.
"""
def distribucion_categorias_con_nulos(df):
    nulos_cat = nulos_categoricos(df)
    for col in nulos_cat:
        print(f"La distribución de las categorías para la columna {col.upper()}:")
        print(df[col].value_counts() / df.shape[0] * 100) 
        print("........................")

"""Creamos función para imputar los nulos categóricos
"""
def imputacion_nulos_categoricos(df):
    columnas_desconocido = ['BusinessTravel', 'EducationField', 'EmployeeNumber',
                            'MaritalStatus', 'OverTime', 'PerformanceRating', 'TotalWorkingYears']
    
    for columna in columnas_desconocido:
        df[columna] = df[columna].fillna('Unknown')
    
    print("Después del reemplazo usando 'fillna', quedan los siguientes nulos:")
    print(df[columnas_desconocido].isnull().sum())

"""Creamos función para ver porcentaje de nulos numéricos.
"""
def nulos_numericos(df):
    nulos_num = df[df.columns[df.isnull().any()]].select_dtypes(include=np.number).columns.tolist()
    return nulos_num

"""Creamos función para ver cómo se distribuyen los valores en esas columnas con nulos numéricos.
"""
def distribucion_categorias_con_nulos_numericos(df):
    nulos_num = nulos_numericos(df)
    for col in nulos_num:
        print(f"La distribución de las categorías para la columna {col.upper()}:")
        print(df[col].value_counts() / df.shape[0] * 100) 
        print("........................")

"""Creamos función para imputar medias a estas tres columnas con nulos numéricos: 'DailyRate', 'HourlyRate'
"""
def imputar_media(df):
    columnas_media=['DailyRate', 'HourlyRate']
    for columna in columnas_media:
        try:
            df[columna].fillna(df[columna].mean(), inplace=True)
        except TypeError as e:
            print(f"Error imputando media para la columna {columna}: {e}")

"""Creamos función para imputar la media a la columna 'WorkLifeBalance' que habíamos convertido a interger.
"""
def imputar_media_worklifebalance(df):
    media_worklifebalance = round(df['WorkLifeBalance'].mean())
    df['WorkLifeBalance'].fillna(media_worklifebalance, inplace=True)

"""Creamos la función para imputar con IterativeImputer la mediana a la columna que falta.
"""
def imputar_mediana(df):
    imputer_iterative = IterativeImputer(max_iter=20, random_state=42)

    # Ajustamos y transformamos los datos para la columna específica
    columna_imputada = imputer_iterative.fit_transform(df[['MonthlyIncome']])

    # Imprimimos el array imputado (opcional)
    print(f"\nArray imputado para la columna '{'MonthlyIncome'}':")
    print(columna_imputada)

    # Asignamos el resultado de vuelta a la columna en el DataFrame original
    df['MonthlyIncome'] = columna_imputada

"""Creamos función para imprimir la info del DF y comprobar que todos los cambios se han aplicado correctamente.
"""
def info(df):
    return df.info()

"""Creamos función para guardar el DF resultante en la carpeta output_data.
"""
def guardar_df(df, nombre_archivo):
    ruta_archivo = os.path.join(os.path.dirname(__file__), '..', 'data', 'output_data', nombre_archivo + '.csv')
    df.to_csv(ruta_archivo, index=False)
    print(f"El DataFrame se ha guardado en {ruta_archivo}")

"""Creamos función para guardar los DF que necesitamos para crear la BBDD.
"""
ruta_df_limpio = '/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETL/data/output_data/df_HR_limpio.csv'

def procesar_y_guardar_csv(input_csv, lista, nombre_archivo, output_folder='data/output_data'):
    df = pd.read_csv(input_csv, usecols=lista)
    ruta_archivo = os.path.join(os.path.dirname(__file__), '..', output_folder, nombre_archivo + '.csv')
    df.to_csv(ruta_archivo, index=False)
    print(f"Archivo guardado correctamente en: {ruta_archivo}")

