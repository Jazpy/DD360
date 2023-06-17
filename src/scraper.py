import requests
import re
import time
from bs4      import BeautifulSoup as bs
from dateutil import parser as date_parser


class MeteoScraper:
    '''
    Clase que representa un scraper que hacer varias llamadas a meteored.mx

    Argumentos:
        urls (lista): lista con URLs que serán consultados.

    Atributos:
        urls (lista): lista con URLs (validados idealmente).
    '''


    def __init__(self, urls=[]):
        '''Validación de URLs e inicializaciones'''
        # TODO: validar URLs bien formados
        self.urls = urls

        # Regex para extraer valor dentro de un span
        self.__span_regex = re.compile('<span id="[^"]+">(.*?)\</span>')


    def __match(self, span):
        '''Utiliza regex que regresa el valor dentro de un span.

        Args:
            span (str): cadena del span del cual queremos extraer un valor.
        Regresa:
            ret (str): cadena representando el valor dentro del span.
        '''
        # Idealmente aquí habría muchos más checks y habría una regex
        # más robusta, o bien usaríamos beautifulsoup para extraer
        # el valor del Tag.
        ret = self.__span_regex.findall(span)
        return ret[0] if ret else None


    def scrape(self):
        '''Consulta los URLs guardados y regresa la información relevante.

        Regresa:
            ret: lista de tuplas de la forma (código respuesta,
                (distancia, fecha y hora, temperatura, humedad)), un elemento
                por cada URL.
        '''
        # Obtener respuestas crudas
        responses = []
        for url in self.urls:
            try:
                # Hacer consulta y guardar código de respuesta.
                # TODO: Tal vez sería bueno hacer esto async.
                response = requests.get(url)
                code = response.status_code

                # Recupera texto solo si fue una consulta exitosa.
                text = response.text if code == 200 else None
            except Exception as e: # Errores de conexión por ejemplo.
                code, text = -1, None
            finally:
                responses.append((code, text))

        ret = []
        for code, text in responses:
            if code == -1 or not text:
                ret.append((code, (None, None, None, None)))
                continue

            # Representación jerárquica del texto.
            parsed = bs(text, 'html.parser')

            # Extraer las cadenas de los valores relevantes.
            dist = self.__match(str(parsed.find(id='dist_cant')))
            temp = self.__match(str(parsed.find(id='ult_dato_temp')))
            humd = self.__match(str(parsed.find(id='ult_dato_hum')))
            updt = self.__match(str(parsed.find(id='fecha_act_dato')))

            # Transformar datos a sus tipos correctos si se recuperaron
            # de manera correcta.
            temp = float(temp) if temp else None
            humd = float(humd) if humd else None

            if updt:
                # Agregar 'Z' al tiempo para indicar UTC y convertir a
                # un objeto datetime.
                updated_dt = date_parser.parse(updt + 'Z')
                # Transformar a timestamp de UNIX.
                updt = int(time.mktime(updated_dt.timetuple()))

            # Agregar a la salida
            ret.append((code, (dist, updt, temp, humd)))

        return ret
