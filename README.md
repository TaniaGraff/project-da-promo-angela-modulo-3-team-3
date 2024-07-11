## Optimización de Talento
Proyecto de análisis de datos y experimentación A/B test desarrollado por la consultoría GET TALENT, para probar hipótesis críticas y proporcionar a ABC Corporation información valiosa sobre sus empleados, con tal de que puedan tomar decisiones estratégicas informadas para la mejora de la gestión de sus recursos humanos.

GET TALENT | *Equipo desarrollo:* Reyes Altozano, Marta García, Tania Graff, Franca Tortarolo

![imagen_portada_modulo](portada.png)

**Cliente** ABC corporation

**Objetivo** Reducir la rotación de empleados y mejorar la satisfacción laboral en la empresa.

**Fases**

- `Fase 1`: Comprender el conjunto de datos y sus características. 

- `Fase 2`: Normalización, transformación y limpieza del conjunto de datos.

- `Fase 3`: Diseño de la estructura de la BBDD, creación de la BBDD, inserción de los datos iniciales. 

- `Fase 4`: Experimentación A/B test para probar hipótesis críticas y determinar que factores tienen una mayor incidencia en la rotación de empleados. 

- `Fase 5`: Proporcionar un informe detallado con las conclusiones de nuestro análisis y las recomendaciones a seguir, utilizando visualizaciones en Python junto con análisis descriptivos y estadísticos.

- `Fase 6`: Automatización de los procesos, desde la limpieza y transformación de los datos, hasta la insercción de datos en la BBDD de empleados de ABC Corporation.

**Estructura de archivos**
- `ETL`
Carpeta que incluye los archivos .py (soporte y main) necesarios para ejecutar la automatización del proceso ETL de limpieza y transformación de datos, así como los archivos .csv de entrada y salida que se han utilizado para dicha automatización.

   *- Ejecución* Para realizar el proceso ETL de la primera fase, ejecutar desde la línea de comandos: python soporte.py | python main.py 

- `ETLII`
Carpeta que incluye los archivos .py (soporte y main) necesarios para ejecutar la automatización del proceso ETL de creación de BBDD e insercción de datos, así como los archivos .csv de entrada y salida que se han utilizado para dicha automatización.

   *- Ejecución* Para realizar el proceso ETL de la segunda fase, ejecutar desde la línea de comandos: python soporte_bbdd.py | python main_bbdd.py

- `Gráficos`
Carpeta que incluye dos archivos .ipynb con las visualizaciones de las variables más relevantes para el análisis.

- `AB_Testing`
Carpeta que incluye 2 archivos .ipynb con las seis pruebas de experimentación A/B test que se han realizado para probar hipótesis críticas y determinar que factores tienen una mayor incidencia en la rotación de empleados.
   - Esta carpeta también contiene el informe con las principales conclusiones de nuestro análisis.

- `Borradores`
Carpeta que incluye los archivos utilizados durante la fase inicial de trabajo, además de los working agreements del equipo.