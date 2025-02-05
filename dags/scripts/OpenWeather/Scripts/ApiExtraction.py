import requests
import os
import pandas as pd
import traceback
from dotenv import load_dotenv
from airflow.models import Variable
API_KEY = Variable.get("API_KEY")


def get_weather_data():
    FilePath = os.path.dirname(__file__)
    os.chdir(FilePath)
    os.chdir('../')
    ProjectPath = os.getcwd()
    InputPath = os.path.join(ProjectPath, r'Input')
    OutputPath = os.path.join(ProjectPath, r'Output')
    LocationsFile = os.path.join(InputPath, r'Locations.xlsx')


    # load_dotenv()
    # API_KEY = os.getenv("API_KEY")

    # Read the locations file
    locations_df = pd.read_excel(LocationsFile)
    print('Locations definidas.')
    output_list = []

    try:
        # Getting weather data
        for _, city in locations_df.iterrows():
            print(f'Se extrae para la ciudad {city}')
            lat = city['Latitud']
            lon = city['Longitud']
            localidad = city['Localidad']
            zona = city['Zona']
            url = f"https://api.openweathermap.org/data/2.5/weather?units=metric&q={localidad}&exclude=minutely,hourly,daily,alerts&appid={API_KEY}&lang=es"
            response = requests.get(url)
            if response.status_code == 200:
                print('API respondio bien')
                weather_data = response.json()
                
                city_info = {
                    'Zona': zona,
                    'Localidad': localidad,
                    'Latitud': weather_data['coord']['lat'],
                    'Longitud': weather_data['coord']['lon'],
                    'Timezone': weather_data['timezone']
                }
                
                output_list.append(city_info)
                print(f'Se termino de extraer data para la ciudad {city}')

        print('Guardando archivo..')
        output_df = pd.DataFrame(output_list)
        # Saving the data
        output_df.to_csv(os.path.join(OutputPath, 'weather_data.csv'), index=False)
        print('El proceso completo exitosamente.')

    except Exception as e:
        print(e)
        traceback.print_exc()
