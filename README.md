# G-DeGo
Proyecto final de Integración de Datos 2022. Facultad de Ingeniería, Universidad de la República.

Se deben instalar las siguientes dependencias mediante el comando pip:
- pandas
- pyodbc
- configparser

Para ejecutar el programa, primero se debe configurar el archivo application.properties. 
[DatabaseSection]
database.servername=[Nombre del servidor que aloja las tablas con los datos cargados]
database.dbname=[Nombre de la base de datos de la cual se leerán los datos y a la cual se cargará la tabla integrada y la tabla de provenance]
database.driver=[Driver]

use.database=[True, si se desea ejecutar utilizando base de datos; False, si se desea realizar todo el flujo utilizando únicamente los archivos .CSV de la carpeta "datos".]

Para ejecutar el programa, se debe correr el comando py main.py. Si se utiliza base de datos pero no está creado el sistema integrado, 
el ETL se correrá automáticamente. En otro caso, se preguntará al usuario si desea correr el ETL. 

Una vez ejecutado, el servidór se alojará en la dirección que se despliega en pantalla.
