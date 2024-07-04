import soporte

print('______________   FASE 1. EXPLORACIÓN INICIAL Y LIMPIEZA DE DATOS   _________________')

print('______________________      APERTURA Y LIMPIEZA HR RAW       _______________________')

#Llamamos a la función para abrir el csv.
df_HR = soporte.lectura_archivo('/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETL/data/input_data/HR_RAW_DATA.csv')
print('Las 5 primeras filas del DataFrame HR son:\n')
print(df_HR.head())
print('....................................................................................')

#Llamamos a la función para explorar los datos.
soporte.exploracion_datos(df_HR)

#Llamamos a la función para eliminar las columnas 'DailyRate', 'HourlyRate', 'MonthlyIncome', 'Over18', 'StandardHours', 'RoleDepartament', 'Department', 'YearsInCurrentRole'.
soporte.eliminar_columnas(df_HR)

#Llamamos a la función para remplazar los valores de la columna Gender.
soporte.reemplazar_valores_genero(df_HR)

#Llamamos a la función para cambiar columnas a float.
soporte.limpiar_y_convertir_a_float(df_HR)

#Llamamos a la función para cambiar columnas a interger.
soporte.limpiar_y_convertir_a_entero(df_HR)

#llamamos a la función para cambiar columna Age a interger.
soporte.corregir_edades(df_HR)

#Llamamos a la función para cambiar valores negativos columna DistanceFromHome.
soporte.sustituir_negativos(df_HR)

#Llamamos a la función para corregir errores tipográficos columna MaritalStatus.
soporte.corregir_errores_maritalstatus(df_HR)

#Llamamos a la función para normalizar y renombrar columnas.
soporte.normalizar_y_renombrar(df_HR) 

print('__________________________     CONTEO FRECUENCIAS    _______________________________')

#Llamamos función para contar frecuencias.
soporte.contar_frequencias(df_HR)


#Llamamos función para eliminar duplicados de la columna EmployeeNumber
soporte.eliminar_duplicados(df_HR)

print('____________________________      GESTIÓN NULOS      _______________________________')

#Llamamos a la función para ver los nulos del DF.
soporte.nulos(df_HR)
print('....................................................................................')

#Llamamos a la función para gestionar los nulos categóricos.
soporte.nulos_categoricos(df_HR)

#Llamamos a la función para ver cómo se disrtibuyen los valores en las columnas con nulos categóricos.
soporte.distribucion_categorias_con_nulos(df_HR)

#Llamamos a la función para imputar los nulos categóricos en una nueva categoría llamada Unknown.
soporte.imputacion_nulos_categoricos(df_HR)

#Llamamos a la función para gestionar los nulos numericos.
soporte.nulos_numericos(df_HR)
print('....................................................................................')

#Llamamos a la función para ver cómo se disrtibuyen los valores en las columnas con nulos numéricos.
soporte.distribucion_categorias_con_nulos_numericos(df_HR)

#Llamamos a la función para imputar las medias en las columnas de 'DailyRate', 'HourlyRate'.
soporte.imputar_media(df_HR)

#Llamamos a la función para imputar la media en la columna 'WorkLifeBalance', de tipo interger.
soporte.imputar_media_worklifebalance(df_HR)

#Llamamos a la función para imputar la mediana en la columna 'MonthlyIncome'.
soporte.imputar_mediana(df_HR)
print('....................................................................................')

#Llamamos a la función info para comprobar que los cambios se han aplicado correctamente.
soporte.info(df_HR)

print('__________________________      CSV DF RESULTANTE      _____________________________')
#Llamamos a la función para guardar el DF limpio resultante.
soporte.guardar_df(df_HR, 'df_HR_limpio')


print('__________________________________    CSV BBDD   ___________________________________')

#Llamamos a la función para guardar los DF que vamos a crear a partir del DF resultante, que necesitamos para la BBDD.
#Definimos las listas de columnas para cada categoría y guardamos.
employee = ['Age', 'Gender', 'MaritalStatus', 'DateBirth', 'Education', 'EducationField', 'Attrition', 'BusinessTravel', 'DistanceFromHome']
job_details = ['JobRole', 'JobLevel', 'NumCompaniesWorked', 'TotalWorkingYears', 'YearsAtCompany', 'YearsSinceLastPromotion', 'YearsWithCurrManager']
compensation = ['DailyRate', 'HourlyRate', 'MonthlyIncome', 'MonthlyRate', 'StockOptionLevel', 'PercentSalaryHike', 'OverTime', 'TrainingTimesLastYear', 'RemoteWork']
satisfaction = ['EnvironmentSatisfaction', 'JobInvolvement', 'JobSatisfaction', 'RelationshipSatisfaction', 'WorkLifeBalance', 'PerformanceRating']

ruta_df_limpio = '/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETL/data/output_data/df_HR_limpio.csv'

soporte.procesar_y_guardar_csv(ruta_df_limpio, employee, 'Employees')
soporte.procesar_y_guardar_csv(ruta_df_limpio, job_details, 'JobDetails')
soporte.procesar_y_guardar_csv(ruta_df_limpio, compensation, 'Compensation')
soporte.procesar_y_guardar_csv(ruta_df_limpio, satisfaction, 'Satisfaction')



