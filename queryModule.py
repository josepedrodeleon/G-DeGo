import pandas as pd
import configparser
from utils import connection
import datasetsETL as dataset
# Set up configs
config = configparser.RawConfigParser()
config.read('application.properties')

if config.get('DatabaseSection', 'use.database') == 'True':
    print("Database connection enabled. Check application.properties to change this")
    conn = connection.set_up_conn()
    dbname = config.get('DatabaseSection', 'database.dbname')
    try :
        result = pd.read_sql(f"SELECT * from {dbname}.dbo.TvShowsAndMoviesWithRating", conn)
    except :
        print("The table TvShowsAndMoviesWithRating will be created, please wait...")
        dataset.run_etl()
        result = pd.read_sql(f"SELECT * from {dbname}.dbo.TvShowsAndMoviesWithRating", conn)
else:
    print("Database connection disabled. Check application.properties to change this")
    try:
        result = pd.read_csv('datos/TvShowsAndMoviesWithRating.csv')
    except:
        print("The CSV TvShowsAndMoviesWithRating will be created, please wait...")
        dataset.run_etl()
        result = pd.read_csv('datos/TvShowsAndMoviesWithRating.csv')

def where_to_watch(content_name):
  indice = result.index[result.title.str.lower() == str(content_name).lower()].tolist()
  available_on = result.loc[indice[0], 'available_on']
  return available_on

def get_content_info(content_name):
    indice = result.index[result.title.str.lower() == str(content_name).lower()].tolist()
    contentinfo = result.loc[indice[0]]
    supuestoJson = row_to_json(contentinfo)
    return supuestoJson

def row_to_json(dataframeRow):
    json = {'title': dataframeRow.title,
            'type': dataframeRow.type,
            'description': dataframeRow.description,
            'release_year': str(dataframeRow.release_year),
            'production_countries' : dataframeRow.production_countries,
            'runtime': dataframeRow.runtime,
            'imdb_rating': dataframeRow.imdb_rating,
            'available_on': dataframeRow.available_on }
    return json

def get_movies_and_shows():
    movies_and_shows = []
    for index, row in result.iterrows():
        json_content = row_to_json(row)
        movies_and_shows.append(json_content)
    return movies_and_shows

def produced_in_uruguay():
    in_uruguay = []
    for index, row in result.iterrows():
        if row.production_countries != None and str(row.production_countries).__contains__("Uruguay"):
            print(row.title, " en uruguay")
            in_uruguay.append(row_to_json(row))
        #in_uruguay = result.loc[result.production_countries != None & result.production_countries.str.contains("Uruguay")]
    print("Producidas en uru: ", in_uruguay)
    return in_uruguay