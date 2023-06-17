import sqlite3
import os
import json
import pandas as pd
import time
from collections import defaultdict
from datetime    import datetime as dt


class DBManager:
    '''
    Clase que maneja comunicación con la BD.

    Atributos:
        db_path (str): path a la BD.
    '''


    def __init__(self, db_path, parquet_path, csv_path):
        '''Inicialización de conexión y objetos JSON.

        Argumentos:
            db_path (str): path a la BD.
            parquet_path (str): path al output de parquet.
            csv_path (str): path al output CSV.
        '''
        self.db_path      = db_path
        self.parquet_path = parquet_path
        self.csv_path     = csv_path

        # Cargar último archivo JSON
        try:
            # TODO: idealmente esto sería mucho más robusto, cosas como
            # guardar en algún lugar cuál es el archivo JSON que debemos
            # procesar, y reglas sobre qué archivos puede haber en este dir.
            all_json = [f'./json/{x}' for x in os.listdir('./json')
                if x.endswith('.json')]
            add_json = max(all_json, key=os.path.getctime)

            with open(add_json, 'r') as in_f:
                self.add_data = json.load(in_f)
        except Exception as e:
            print('Error leyendo JSON!')
            raise IOError


    def __add_response(self, city, dist, temp, humd, ts, q_id, code):
        '''Agrega los datos asociados a una consulta a la BD

        Argumentos:
            city (str): ciudad objeto.
            dist (str): distancia de estación.
            temp (float): temperatura.
            humd (float): humedad.
            ts (int): tiempo de actualización.
            q_id (str): id único de la consulta.
            code (int): código de respuesta.

        Arroja:
            ConnectionError: no se pudo conectar a la BD.
            SQLite3.Error: error al insertas filas.
        '''
        # Conexión a SQLite
        try:
            conn = sqlite3.connect(self.db_path)
        except Exception as e:
            print('Error conectándose a la BD!')
            raise ConnectionError

        # Agregar ciudad si es necesario
        cursor = conn.execute('select id from cities where '
            f'name="{city}" limit 1')
        row = cursor.fetchone()
        if not row:
            conn.execute(f'insert into cities (name) values ("{city}")')
            cursor = conn.execute('select id from cities where '
                f'name="{city}" limit 1')
            row = cursor.fetchone()
        city_id = row[0]

        # Agregar a tabla de códigos
        conn.execute('insert into codes (id, code) values '
            f'("{q_id}", {code})')

        # TODO: agregar sanitizador que haga esto de manera más general.
        # Agregar a tabla principal de consultas
        dist = f'"{dist}"' if dist else 'NULL'
        temp = temp if temp else 'NULL'
        humd = humd if humd else 'NULL'
        ts   = ts   if ts   else 'NULL'
        conn.execute('insert into reports (distance, temperature, '
            'humidity, updated, code_id, city_id) values '
            f'({dist}, {temp}, {humd}, {ts}, "{q_id}", {city_id})')

        conn.commit()
        conn.close()


    def __get_checkpoint(self):
        '''Recupera los datos para un checkpoint de parquet en un diccionario

        Regresa:
            ret (dictionary): diccionario con reporte promedio y más reciente
            para cada ciudad, formato para pandas.

        Arroja:
            ConnectionError: no se pudo conectar a la BD.
            SQLite3.Error: error al buscar filas.
        '''
        # Conexión a SQLite
        try:
            conn = sqlite3.connect(self.db_path)
        except Exception as e:
            print('Error conectándose a la BD!')
            raise ConnectionError

        ret = defaultdict(list)

        # Iteramos sobre cada ciudad, construyendo un reporte a la vez
        cursor = conn.execute('select * from cities')
        rows   = cursor.fetchall()
        for row in rows:
            c_id = row[0]
            ret['ciudad'].append(row[1])

            # Temperatura mínima, máxima, y promedio
            cursor = conn.execute(f'select max(temperature) from reports where city_id={c_id}')
            ret['max_temp'].append(cursor.fetchone()[0])
            cursor = conn.execute(f'select min(temperature) from reports where city_id={c_id}')
            ret['min_temp'].append(cursor.fetchone()[0])
            cursor = conn.execute(f'select avg(temperature) from reports where city_id={c_id}')
            ret['avg_temp'].append(cursor.fetchone()[0])

            # Humedad mínima, máxima, y promedio
            cursor = conn.execute(f'select max(humidity) from reports where city_id={c_id}')
            ret['max_humid'].append(cursor.fetchone()[0])
            cursor = conn.execute(f'select min(humidity) from reports where city_id={c_id}')
            ret['min_humid'].append(cursor.fetchone()[0])
            cursor = conn.execute(f'select avg(humidity) from reports where city_id={c_id}')
            ret['avg_humid'].append(cursor.fetchone()[0])

            # Valores más recientes
            cursor = conn.execute(f'select max(updated) from reports where city_id={c_id}')
            ts     = cursor.fetchone()[0]

            if ts:
                cursor = conn.execute(f'select temperature, humidity from reports where updated={ts}')
                row    = cursor.fetchone()
                curr_t, curr_h = row[0], row[1]
            else:
                curr_t, curr_h = None, None

            ret['curr_temp'].append(curr_t)
            ret['curr_humid'].append(curr_h)
            ret['updated'].append(ts)

        conn.close()
        return ret


    def __write_parquet(self):
        '''Escribe un archivo parquet particionado por tiempo de la corrida,
        con valores mínimos, máximos, promedio, y más recientes al momento
        de la corrida para cada ciudad. Hacer lo mismo para un archivo CSV.
        '''
        try:
            data = self.__get_checkpoint()
        except Exception as e:
            print('Error leyendo de la BD!')
            return

        # Pasar diccionario a pandas y escribir el archivo
        df = pd.DataFrame(data)

        # Escribir archivos de salida con nombre único.
        ts = int(dt.utcnow().timestamp())
        try:
            df.to_parquet(f'{self.parquet_path}/{ts}.parquet',
                partition_cols=['updated'])
            df.to_csv(f'{self.csv_path}/{ts}.csv')
        except Exception as e:
            print('Error escribiendo Parquet/CSV!')
            raise e
            return


    def commit_latest(self):
        '''Agrega las consultas almacenadas en el archivo JSON más reciente

        Arroja:
            IOError: no se pudo cargar el JSON más reciente.
        '''
        # Cargar último archivo JSON
        try:
            # TODO: idealmente esto sería mucho más robusto, cosas como
            # guardar en algún lugar cuál es el archivo JSON que debemos
            # procesar, y reglas sobre qué archivos puede haber en este dir.
            all_json = [f'./json/{x}' for x in os.listdir('./json')
                if x.endswith('.json')]
            add_json = max(all_json, key=os.path.getctime)

            with open(add_json, 'r') as in_f:
                add_data = json.load(in_f)
        except Exception as e:
            print('Error leyendo JSON!')
            raise IOError

        success = False
        for query in add_data:
            try:
                self.__add_response(query['city'], query['distance'],
                    query['temperature'], query['humidity'], query['update'],
                    query['id'], query['code'])
                success = True
            except Exception as e:
                print(f'Error modificando BD, el reporte con id={query["id"]}'
                    ' no fue agregado.')

        # Solo tiene sentido escribir un archivo nuevo si hubo algún cambio.
        if success:
            self.__write_parquet()


def main():
    '''Crea un objecto DBManager y llama su método principal'''
    DBManager('db/meteo.db', './parquet/', './csv/').commit_latest()


if __name__ == '__main__':
    main()
