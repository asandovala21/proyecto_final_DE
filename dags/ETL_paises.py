from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import numpy as np
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os

dag_path = os.getcwd()     #path original.. home en Docker

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
with open(f'{dag_path}/keys/db.txt', 'r') as f:
    data_base= f.read()
with open(f'{dag_path}/keys/user.txt', 'r') as f:
    user= f.read()
with open(f'{dag_path}/keys/pwd.txt', 'r') as f:
    pwd= f.read()

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'AndreaSA',
    'start_date': datetime(2023,6,26),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='ETL_paises',
    default_args=default_args,
    description='Agrega data de países de forma mensual',
    schedule_interval="@monthly",
    catchup=False
)

dag_path = os.getcwd()     #path original.. home en Docker

# funcion de extraccion de datos
def extraer_data(exec_date):
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        #API de datos de países
        url = "https://restcountries.com/v3.1/all"
        response = requests.get(url)
        if response:
            print('Success!')
            
            data=response.json()
            
            nombre_comun= [data[x].get('name').get('common')
                    for x in range(len(data))]
            nombre_oficial= [data[x].get('name').get('official')
                    for x in range(len(data))]
            pais_independiente= [data[x].get('independent')
                    for x in range(len(data))]
            moneda_nombre = [list(data[x].get('currencies').values())[0].get('name')
                            if data[x] is not None and data[x].get('currencies') is not None
                            else None
                            for x in range(len(data))]
            moneda_simbolo= [list(data[x].get('currencies').keys())[0]
                            if data[x] is not None and data[x].get('currencies') is not None
                            else None
                    for x in range(len(data))]
            capital= [data[x].get('capital')[0]
                    if data[x] is not None and data[x].get('capital') is not None
                            else None
                    for x in range(len(data))]
            region= [data[x].get('region')
                    for x in range(len(data))]
            subregion= [data[x].get('subregion')
                    for x in range(len(data))]
            continente= [data[x].get('continents')[0]
                    for x in range(len(data))]
            lenguaje=[list(data[x].get('languages').values())[0]
                    if data[x] is not None and data[x].get('languages') is not None
                            else None
                    for x in range(len(data))]
            url_googe_maps=[data[x].get('maps').get('googleMaps')
                    for x in range(len(data))]
            poblacion=[data[x].get('population')
                    for x in range(len(data))]
            gini_valor=[list(data[x].get('gini').values())[0]
                        if data[x] is not None and data[x].get('gini') is not None
                            else None
                    for x in range(len(data))]
            gini_year=[list(data[x].get('gini').keys())[0]
                    if data[x] is not None and data[x].get('gini') is not None
                            else None
                    for x in range(len(data))]

            df = pd.DataFrame({'Nombre_comun': nombre_comun,'Nombre_oficial': nombre_oficial,'Pais_independiente': pais_independiente,'Moneda_nombre': moneda_nombre,'Moneda_simbolo': moneda_simbolo,'Capital': capital,'Region': region,'Subregion': subregion,'Continente': continente,'Lenguaje': lenguaje,'Url_googe_maps': url_googe_maps,'Poblacion': poblacion,'Gini_valor': gini_valor,'Gini_year': gini_year})

            df.to_csv(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False)
        else:
            print('An error has occurred.')
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e       

# Funcion de transformacion en tabla
def transformar_data(exec_date):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    dt = pd.read_csv(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    # Fiiltrar el topico de interes: mining_stats
   #Explorando el dataframe
    df.head()
    #Revisar los tipos de datos
    df.dtypes
    # #Cambiar el tipo de dato de la columna poblacion a int
    # df['poblacion'] = df['poblacion'].astype('int64')
    # #Cambiar el tipo de dato de la columna gini valor a float
    # df['gini_valor'] = df['gini_valor'].astype('float64')
    df['Pais_independiente'] = df['Pais_independiente'].astype('bool').astype('int64')
    #Revisar si hay duplicados
    df.duplicated().sum()
    #Revisar otra vez si hay nulos
    df.isnull().sum()
    df.info()
    df.describe()
    df = df.drop_duplicates()

    df_2=df.groupby('Subregion').agg(suma_poblacion=('Poblacion',np.sum), cantidad_paises=('Nombre_comun', np.size),n_paises_independientes=('Pais_independiente',np.sum),pais_max_poblacion=('Poblacion',np.max)).reset_index()

    df_aux=df[['Nombre_comun','Poblacion']]
    df_2=df_2.merge(df_aux,how='left',left_on=['pais_max_poblacion'],right_on=['Poblacion'])
    df_2.drop(columns=['Poblacion'],inplace=True)
    df_2.rename(columns={'Nombre_comun':'pais_max_poblacion_nombre'},inplace=True)
    df_2.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False)
    del(df_aux)

# Funcion conexion a redshift
def conexion_redshift(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
    #engine = create_engine(f'redshift+psycopg2://{redshift_conn["username"]}:{redshift_conn["pwd"]}@{redshift_conn["host"]}:{redshift_conn["port"]}/{redshift_conn["database"]}')
    #print(engine)

from psycopg2.extras import execute_values
# Funcion de envio de data
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    #date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    dataframe=pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    print(dataframe.shape)
    print(dataframe.head())
    # conexion a database
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')
    # Definir columnas
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT', 'int32': 'INT', 'float64': 'FLOAT',
                'object': 'VARCHAR(50)', 'bool': 'BOOLEAN', 'datetime64[ns, UTC]': 'TIMESTAMP'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    sql_dtypes = [x.replace('VARCHAR(50)', f'VARCHAR({len(max(dataframe[y].fillna(""),key=len))})')
                  if x == 'VARCHAR(50)' else x for x, y in zip(sql_dtypes, cols)]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name,
                   data_type in zip(cols, sql_dtypes)]
    name=cols[0]
    primary_key=[f"primary key({name})"]
    column_defs.append(primary_key[0])
    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    # Crear la tabla
    from psycopg2.extras import execute_values
    cur = conn.cursor()
    table_name = 'base_paises_resumen'
    cur.execute(table_schema)
    # Generar los valores a insertar
    values = [tuple(x) for x in dataframe.to_numpy()]
    
    # for i in range(4):
    #     print(f'Valores de {i}')
    #     for j in range(len(values)):
    #         print(len(values[j][i]))
        
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')
    
# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32