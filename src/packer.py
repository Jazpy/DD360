import json
from scraper  import MeteoScraper
from datetime import datetime as dt


class Packer:
    '''
    Clase que pide información actualizada y la guarda como archivos JSON.

    Atributos:
        cities (lista): lista con ciudades de interés.
        urls (lista): lista con URLs formadas a partir de "cities".
        scraper (MeteoScraper): scraper para meteored.mx
    '''


    def __init__(self):
        '''Inicialización de ciudades de interés'''
        # TODO: idealmente todo esto vendría de un archivo de configuración
        self.cities = ['ciudad-de-mexico', 'monterrey', 'merida', 'wakanda']
        self.urls = [f'https://www.meteored.mx/{c}/historico' for
            c in self.cities]
        self.scraper = MeteoScraper(self.urls)


    def pack(self):
        '''Llama al scraper y guarda resultados como archivos JSON

        Arroja:
            EnvironmentError: no se pudo escribir al archivo JSON.
        '''
        # Para el ID de estos queries.
        ts = int(dt.utcnow().timestamp())

        # Hacer queries a meteored.
        responses = self.scraper.scrape()

        # Transformar a diccionarios.
        dics = []
        for i, (city, query, response) in enumerate(
            zip(self.cities, self.urls, responses)):
            response_dic = {
                'query':       query,
                'city':        city,
                'code':        response[0],
                'distance':    response[1][0],
                'update':      response[1][1],
                'temperature': response[1][2],
                'humidity':    response[1][3],
                'run':         ts,
                'id':          f'{ts}_{i}'
            }

            dics.append(response_dic)

        # Guardar diccionarios en un archivo JSON.
        try:
            with open(f'json/{ts}.json', 'w') as out_f:
                out_f.write(json.dumps(dics))
        except EnvironmentError as e:
            print('Error escribiendo JSON!')
            raise e


def main():
    '''Crea un objecto Packer y llama su método principal'''
    Packer().pack()


if __name__ == '__main__':
    main()
