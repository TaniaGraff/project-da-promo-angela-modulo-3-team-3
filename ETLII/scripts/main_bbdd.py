import soporte_bbdd
print('__________________   FASE 2. CREACIÓN BBDD ABC CORPORATION   ______________________')

print('__________________      APERTURA CSV Y RESETEO ÍNDICES      _______________________')
print('...................     CSV EMPLOYEES')

#Llamamos a la función para abrir el CSV EMPLOYEES.
df_employees= soporte_bbdd.lectura_archivo('../../ETL/data/output_data/Employees.csv')

#Llamamos a la función para resetear el índice.
soporte_bbdd.resetear_indice_y_renombrar(df_employees)

#Llamamos a la función para guardar el DF EMPLOYEES.
soporte_bbdd.guardar_df(df_employees, 'Employees')

print('...................     CSV JOB DETAILS')
df_job_details= soporte_bbdd.lectura_archivo('../../ETL/data/output_data/JobDetails.csv')

#Llamamos a la función para resetear el índice.
soporte_bbdd.resetear_indice_y_renombrar(df_job_details)

#Llamamos a la función para guardar el DF JOB DETAILS.
soporte_bbdd.guardar_df(df_job_details, 'JobDetails')

print('...................     CSV COMPENSATION')
df_compensation= soporte_bbdd.lectura_archivo('../../ETL/data/output_data/Compensation.csv')

#Llamamos a la función para resetear el índice.
soporte_bbdd.resetear_indice_y_renombrar(df_compensation)

#Llamamos a la función para guardar el DF JOB DETAILS.
soporte_bbdd.guardar_df(df_compensation, 'Compensation')

print('...................     CSV SATISFACTION')
df_satisfaction= soporte_bbdd.lectura_archivo('../../ETL/data/output_data/Satisfaction.csv')

#Llamamos a la función para resetear el índice.
soporte_bbdd.resetear_indice_y_renombrar(df_satisfaction)

#Llamamos a la función para guardar el DF JOB DETAILS.
soporte_bbdd.guardar_df(df_satisfaction, 'Satisfaction')

print('___________________________      CREACIÓN BBDD      _______________________________')
#Llamamos a la función para crear BBDD.
soporte_bbdd.crear_base_de_datos('root', 'AlumnaAdalab')

print('__________________________      CREACIÓN TABLAS      ______________________________')
print('...................    TABLA EMPLOYEES')
soporte_bbdd.crear_tabla_empleados('root', 'AlumnaAdalab')

print('...................    TABLA DETAILS')
soporte_bbdd.crear_tabla_job_details('root', 'AlumnaAdalab')

print('...................    TABLA COMPENSATION')
soporte_bbdd.crear_tabla_compensation('root', 'AlumnaAdalab')

print('...................    TABLA SATISFACTION')
soporte_bbdd.crear_tabla_satisfaction('root', 'AlumnaAdalab')

print('__________________________      INSERCIÓN DATOS      ______________________________')

print('...................    TABLA EMPLOYEES')
query = """
    INSERT INTO Employees (Age, Attrition, BusinessTravel, DistanceFromHome, Education, EducationField, Gender, MaritalStatus, DateBirth
)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

file_path = '../data/output_data/Employees.csv'
soporte_bbdd.insertar_datos(query, user='root', password='AlumnaAdalab', host='127.0.0.1', database='ABC_Corporation', file_path=file_path)
print('___________________________________________________________________________________')

print('...................    TABLA DETAILS')
query = """
    INSERT INTO JobDetails (JobLevel, JobRole, NumCompaniesWorked, TotalWorkingYears, YearsAtCompany, YearsSinceLastPromotion, YearsWithCurrManager)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
file_path = '../data/output_data/JobDetails.csv'
soporte_bbdd.insertar_datos(query, user='root', password='AlumnaAdalab', host='127.0.0.1', database='ABC_Corporation', file_path=file_path)
print('___________________________________________________________________________________')

print('...................    TABLA COMPENSATION')
query = """
    INSERT INTO Compensation (DailyRate, HourlyRate, MonthlyIncome, MonthlyRate, OverTime, PercentSalaryHike, StockOptionLevel, TrainingTimesLastYear, RemoteWork)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

file_path = '../data/output_data/Compensation.csv'
soporte_bbdd.insertar_datos(query, user='root', password='AlumnaAdalab', host='127.0.0.1', database='ABC_Corporation', file_path=file_path)
print('___________________________________________________________________________________')

print('...................    TABLA SATISFACTION')
query = """
    INSERT INTO Satisfaction (EnvironmentSatisfaction, JobInvolvement, JobSatisfaction, PerformanceRating, RelationshipSatisfaction, WorkLifeBalance)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
file_path = '../data/output_data/Satisfaction.csv'
soporte_bbdd.insertar_datos(query, user='root', password='AlumnaAdalab', host='127.0.0.1', database='ABC_Corporation', file_path=file_path)
