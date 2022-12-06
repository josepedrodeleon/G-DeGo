import numpy as np
import pandas as pd
import pyodbc as odbc 
import countries

DRIVER_NAME ='ODBC Driver 17 for SQL Server'
SERVER_NAME ='IBM-PF3HTZAK\SQLEXPRESS'
DATABASE_NAME ='StreamingContent'

connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trust_Connection=yes;
"""
conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'SERVER=IBM-PF3HTZAK\SQLEXPRESS;'
                      'DATABASE=StreamingContent;'
                      'Trusted_connection=yes;')
# conexion con db
netflix = pd.read_sql("SELECT * from netflix", conn)[['title', 'type', 'release_year', 'age_certification', 'production_countries', 'runtime', 'seasons']]
disney = pd.read_sql("SELECT * from disney_plus_titles", conn)[['title', 'type', 'release_year', 'rating', 'country', 'duration']]
imdbMovies = pd.read_sql("SELECT * from imdb_top_1000", conn)
imdbSeries = pd.read_sql("SELECT * from IMDbSeries", conn)

#netflix = pd.read_csv('datos/titles.csv')[['title', 'type', 'release_year', 'age_certification', 'production_countries', 'runtime', 'seasons']]
#disney = pd.read_csv('datos/disney_plus_titles.csv')[['title', 'type', 'release_year', 'rating', 'country', 'duration']]
disney.columns = ['title', 'type', 'release_year', 'age_certification', 'production_countries', 'runtime']
available_on = []

netflix["imdb_rating"] = ""
print("IMDB: ", netflix)#imdbSeries[['Series_Title']].values)
# Estructura auxiliar para controlar similaridad de nombres
disney_titles_only = disney[['title']]
for index, row in disney_titles_only.iterrows():
    disney_titles_only.at[index, 'title'] = row.title.lower()

# Transformacion de Netflix, limpieza
for index, row in netflix.iterrows():
    netflix.at[index, 'production_countries'] = countries.netflix_to_disney(row.production_countries)
    if row.type == 'SHOW':
        netflix.at[index, 'type'] = 'TV Show'
        if row.seasons == 1:
            netflix.at[index, 'runtime'] = str(int(row.seasons)) + ' Season'
        else:
            netflix.at[index, 'runtime'] = str(int(row.seasons)) + ' Seasons'
        #add imdb series score to netflix data set
        if row.title in imdbSeries[['Series_Title']].values:
            indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
            netflix.at[index, "imdb_rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
    else:
        netflix.at[index, 'type'] = 'Movie'
        netflix.at[index, 'runtime'] = str(row.runtime) + ' min'
        #add imdb movies score to netflix data set
        if row.title in imdbMovies[['Series_Title']].values:
            indice = imdbMovies.index[imdbMovies['Series_Title'] == row.title].tolist()
            netflix.at[index, "imdb_rating"] = imdbMovies.loc[indice[0], 'IMDB_Rating']
    if str(row.title).lower() in disney_titles_only['title'].values:
        available_on.append("Netflix, Disney+")
        indice = disney_titles_only.index[disney_titles_only['title'] == str(row.title).lower()].tolist()
        disney.drop(index=indice[0], inplace=True)
    else:
        available_on.append("Netflix")
    
for index, row in disney.iterrows():
    #add imdb series score to disney data set
    if row.type == 'SHOW':
        if row.title in imdbSeries[['Series_Title']].values:
            indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
            disney.at[index, "imdb_rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
    #add imdb movies score to disney data set        
    else:
        if row.title in imdbMovies[['Series_Title']].values:
            indice = imdbMovies.index[imdbMovies['Series_Title'] == row.title].tolist()
            disney.at[index, "imdb_rating"] = imdbMovies.loc[indice[0], 'IMDB_Rating']

netflix = netflix.assign(available_on=available_on).drop('seasons', axis=1)
available_on = ['Disney'] * disney['title'].values.size
disney = disney.assign(available_on=available_on)

result = pd.concat([netflix, disney], ignore_index=True)

print(result)
result.to_csv('datos/resultado.csv')

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
            'release_year': dataframeRow.release_year, 
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