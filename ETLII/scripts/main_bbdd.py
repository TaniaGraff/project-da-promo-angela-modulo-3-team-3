import soporte_bbdd
print('__________________   FASE 2. CREACIÓN BBDD ABC CORPORATION   ______________________')

print('__________________      APERTURA CSV Y RESETEO ÍNDICES      _______________________')
print('...................     CSV EMPLOYEES')

#Llamamos a la función para abrir el CSV EMPLOYEES.
df_employees= soporte_bbdd.lectura_archivo('/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETL/data/output_data/Employees.csv')

#Llamamos a la función para resetear el índice.
soporte_bbdd.resetear_indice_y_renombrar(df_employees)

#Llamamos a la función para guardar el DF EMPLOYEES.
soporte_bbdd.guardar_df(df_employees, 'Employees')

print('...................     CSV JOB DETAILS')
df_job_details= soporte_bbdd.lectura_archivo('/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETL/data/output_data/JobDetails.csv')

#Llamamos a la función para resetear el índice.
soporte_bbdd.resetear_indice_y_renombrar(df_job_details)

#Llamamos a la función para guardar el DF JOB DETAILS.
soporte_bbdd.guardar_df(df_job_details, 'JobDetails')

print('...................     CSV COMPENSATION')
df_compensation= soporte_bbdd.lectura_archivo('/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETL/data/output_data/Compensation.csv')

#Llamamos a la función para resetear el índice.
soporte_bbdd.resetear_indice_y_renombrar(df_compensation)

#Llamamos a la función para guardar el DF JOB DETAILS.
soporte_bbdd.guardar_df(df_compensation, 'Compensation')

print('...................     CSV SATISFACTION')
df_satisfaction= soporte_bbdd.lectura_archivo('/Users/Tania_1/Desktop/ADALAB/MODULO_3/project-da-promo-angela-modulo-3-team-3/ETL/data/output_data/Satisfaction.csv')

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

print('...................     TABLA COMPENSATION')
soporte_bbdd.crear_tabla_compensation('root', 'AlumnaAdalab')

print('...................     TABLA SATISFACTION')
soporte_bbdd.crear_tabla_satisfaction('root', 'AlumnaAdalab')

