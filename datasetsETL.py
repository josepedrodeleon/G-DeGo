import configparser
import pandas as pd
from utils import countries, connection

# Set up configs
config = configparser.RawConfigParser()
config.read('application.properties')

def extract():
    print("Extracting...")
    # Extract
    # Set up connection or read from csvs
    database_enabled = config.get('DatabaseSection', 'use.database') == 'True'
    if database_enabled:
        print("Connection with database enabled. Reading data from SQL")
        # conexion con db
        conn = connection.set_up_conn()
        netflix = pd.read_sql("SELECT * from netflix", conn)[
            ['title', 'type', 'description', 'release_year', 'age_certification', 'production_countries', 'runtime', 'seasons']]
        disney = pd.read_sql("SELECT * from disney_plus_titles", conn)[
            ['title', 'type', 'description', 'release_year', 'rating', 'country', 'duration']]
        imdbMovies = pd.read_sql("SELECT * from imdb_top_1000", conn)
        imdbSeries = pd.read_sql("SELECT * from IMDbSeries", conn)
    else:
        print("Connection with database disabled. Reading data from CSV")
        netflix = pd.read_csv('datos/titles.csv')[
            ['title', 'type', 'description', 'release_year', 'age_certification', 'production_countries', 'runtime', 'seasons']]
        disney = pd.read_csv('datos/disney_plus_titles.csv')[
            ['title', 'type', 'description', 'release_year', 'rating', 'country', 'duration']]
        imdbMovies = pd.read_csv('datos/imdb_top_1000.csv')
        imdbSeries = pd.read_csv('datos/series_data.csv')

    return netflix, disney, imdbMovies, imdbSeries

def transform(netflix, disney, imdbMovies, imdbSeries):
    print("Tansforming...")

    #building provenance table
    provenance_columns = ["N.title", "N.type", "N.release_year", "N.age_certification", "N.production_countries", 
    "N.runtime", "N.seasons", "D.title", "D.type", "D.release_year", "D.rating", "D.country", "D.duration", 
    "IMDBShows.Series_Title", "IMDBShows.IMDB_Rating", "IMDBMovies.IMDB_Rating","IMDBMovies.Series_Title", 
    "TvShowsAndMoviesWithRating.title", "TvShowsAndMoviesWithRating.type", "TvShowsAndMoviesWithRating.release_year", 
    "TvShowsAndMoviesWithRating.age_certification", "TvShowsAndMoviesWithRating.productionCountry", 
    "TvShowsAndMoviesWithRating.runtime"]
  #  provenance_columns = {"N.title": netflix.title, "N.type": netflix.type, "N.release_year": netflix.release_year, 
  #                       "N.age_certification": netflix.age_certification, "N.production_countries" : netflix.production_countries, 
  #                      "N.runtime": netflix.runtime,"N.seasons": netflix.seasons, "D.title": disney.title, "D.type": disney.type,
  #                       "D.release_year": disney.release_year, "D.rating" : disney.rating, "D.country": disney.country, 
  #                       "D.duration": disney.duration, "IMDBShows.Series_Title": imdbSeries.Series_Title, "IMDBShows.IMDB_Rating": imdbSeries.IMDB_Rating, 
  #                       "IMDBMovies.Series_Title": imdbMovies.Series_Title, "IMDBMovies.IMDB_Rating": imdbMovies.IMDB_Rating} 
    provenance = pd.DataFrame(columns = provenance_columns)
    provenance2 = pd.DataFrame(columns = provenance_columns)
    # Rename Disney columns to match Netflix, set up auxiliary structures
    #disney.columns = ['title', 'type', 'description', 'release_year', 'age_certification', 'production_countries', 'runtime']
    available_on = []
    netflix["imdb_rating"] = "N/A"
    disney["imdb_rating"] = "N/A"

    # Auxiliary structure to check names
    disney_titles_only = disney[['title']]
    for index, row in disney_titles_only.iterrows():
        disney_titles_only.at[index, 'title'] = row.title.lower()

    # Transformacion de Netflix, limpieza
    for index, row in netflix.iterrows():
        provenance.at[index, "N.title"] = row.title
        provenance.at[index, "N.description"] = row.description
        provenance.at[index, "N.production_countries"] = row.production_countries
        provenance.at[index, "N.type"] = row.type
        provenance.at[index, "N.release_year"] = row.release_year
        provenance.at[index, "N.age_certification"] = row.age_certification
        provenance.at[index, "N.runtime"] = row.runtime
        provenance.at[index, "N.seasons"] = row.seasons
        provenance.at[index, "TvShowsAndMoviesWithRating.title"] = row.title
        provenance.at[index, "TvShowsAndMoviesWithRating.release_year"] = row.release_year
        provenance.at[index, "TvShowsAndMoviesWithRating.age_certification"] = row.age_certification
        provenance.at[index, "TvShowsAndMoviesWithRating.description"] = row.description

        provenance.at[index, "TvShowsAndMoviesWithRating.production_countries"] = countries.netflix_to_disney(row.production_countries)
        if row.type == 'SHOW':
            provenance.at[index, "TvShowsAndMoviesWithRating.type"] = 'TV Show'

            if row.seasons == 1:
                netflix.at[index, 'runtime'] = str(int(row.seasons)) + ' Season'
            else:
                netflix.at[index, 'runtime'] = str(int(row.seasons)) + ' Seasons'
            # add imdb series score to netflix data set
            if row.title in imdbSeries[['Series_Title']].values:
                indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
                provenance.at[index, "IMDBShows.Series_Title"] = row.title
                provenance.at[index, "IMDBShows.IMDB_Rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
        else:
            provenance.at[index, "TvShowsAndMoviesWithRating.type"] = "Movie"
            provenance.at[index, "TvShowsAndMoviesWithRating.runtime"] = str(row.runtime) + ' min'
            # add imdb movies score to netflix data set
            if row.title in imdbMovies[['Series_Title']].values:
                indice = imdbMovies.index[imdbMovies['Series_Title'] == row.title].tolist()
                provenance.at[index, "IMDBMovies.Series_Title"] = row.title
                provenance.at[index, "IMDBMovies.IMDB_Rating"] = imdbMovies.loc[indice[0], 'IMDB_Rating']

        if str(row.title).lower() in disney_titles_only['title'].values:
            available_on.append("Netflix, Disney+")
            indice = disney_titles_only.index[disney_titles_only['title'] == str(row.title).lower()].tolist()
            provenance.at[index, "D.title"] = disney.loc[indice[0], 'title']
            provenance.at[index, "TvShowsAndMoviesWithRating.available_on"] = "Netflix, Disney+"
            disney.drop(index=indice[0], inplace=True)
        else:
            provenance.at[index, "TvShowsAndMoviesWithRating.available_on"] = "Netflix"
            available_on.append("Netflix")

    for index, row in disney.iterrows():

        provenance2.at[index, "D.title"] = row.title
        provenance2.at[index, "D.type"] = row.type
        provenance2.at[index, "D.description"] = row.description
        provenance2.at[index, "D.release_year"] = row.release_year
        provenance2.at[index, "D.rating"] = row.rating
        provenance2.at[index, "D.country"] = row.country
        provenance2.at[index, "D.duration"] = row.duration
        provenance2.at[index, "TvShowsAndMoviesWithRating.title"] = row.title
        provenance2.at[index, "TvShowsAndMoviesWithRating.type"] = row.type
        provenance2.at[index, "TvShowsAndMoviesWithRating.description"] = row.description
        provenance2.at[index, "TvShowsAndMoviesWithRating.release_year"] = row.release_year
        provenance2.at[index, "TvShowsAndMoviesWithRating.age_certification"] = row.rating
        provenance2.at[index, "TvShowsAndMoviesWithRating.production_countries"] = row.country
        provenance2.at[index, "TvShowsAndMoviesWithRating.runtime"] = row.duration
        # add imdb series score to disney data set
        if row.type == 'TV Show':
            if row.title in imdbSeries[['Series_Title']].values:
                indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
                provenance2.at[index, "IMDBShows.Series_Title"] = row.title
                provenance2.at[index, "IMDBShows.IMDB_Rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
                provenance2.at[index, "TvShowsAndMoviesWithRating.imdb_rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
        # add imdb movies score to disney data set
        else:
            if row.title in imdbMovies[['Series_Title']].values:
                indice = imdbMovies.index[imdbMovies['Series_Title'] == row.title].tolist()
                provenance2.at[index, "IMDBMovies.Series_Title"] = row.title
                provenance2.at[index, "IMDBMovies.IMDB_Rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
                provenance2.at[index, "TvShowsAndMoviesWithRating.imdb_rating"] = imdbMovies.loc[indice[0], 'IMDB_Rating']

    netflix = netflix.assign(available_on=available_on).drop('seasons', axis=1)
    provenance2['TvShowsAndMoviesWithRating.available_on'] = 'Disney+'
    provenance_result = pd.concat([provenance, provenance2], ignore_index=True)      

    result = provenance_result [['TvShowsAndMoviesWithRating.title', 'TvShowsAndMoviesWithRating.type','TvShowsAndMoviesWithRating.description',
                        'TvShowsAndMoviesWithRating.release_year', 'TvShowsAndMoviesWithRating.age_certification', 
                       'TvShowsAndMoviesWithRating.production_countries', 'TvShowsAndMoviesWithRating.runtime', 
                       'TvShowsAndMoviesWithRating.imdb_rating',
                       'TvShowsAndMoviesWithRating.available_on']]
    print("PROVENANCE!: ")
   
    result.columns = ['title', 'type', 'description', 'release_year', 'age_certification', 'production_countries', 'runtime', 'imdb_rating', 'available_on']  
    print(result)
    return result

def load(result):
    print("Loading...")
    database_enabled = config.get('DatabaseSection', 'use.database') == 'True'
    if database_enabled:
        dbname = config.get('DatabaseSection', 'database.dbname')
        conn = connection.set_up_conn()
        cursor = conn.cursor()
        cursor.execute(f""" IF OBJECT_ID(N'{dbname}.dbo.TvShowsAndMoviesWithRating', N'U') IS NOT NULL  
                      DROP TABLE {dbname}.dbo.TvShowsAndMoviesWithRating""")
        #IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TvShowsAndMoviesWithRating' and xtype='U'
        conn.commit()
        cursor.execute(f"""CREATE TABLE TvShowsAndMoviesWithRating (
                            title VARCHAR(200),
                            type VARCHAR(10),
                            description VARCHAR(2000),
                            release_year INT,
                            age_certification VARCHAR(50),
                            production_countries VARCHAR(500),
                            runtime VARCHAR(50),
                            imdb_rating VARCHAR(10),
                            available_on VARCHAR(50)
                            )
                            """)
        conn.commit()
        for index, row in result.iterrows():
            cursor.execute(f"INSERT INTO {dbname}.dbo.TvShowsAndMoviesWithRating (title,type,description,release_year,age_certification,\
                            production_countries,runtime,imdb_rating,available_on) values(?,?,?,?,?,?,?,?,?)",
                           row.title, row.type, row.description, row.release_year, row.age_certification, row.production_countries,
                           row.runtime, row.imdb_rating, row.available_on)
        conn.commit()
        cursor.close()
    else:
        result.to_csv('datos/resultado.csv')

def build_provenance_table(netflix, disney, imdbMovies, imdbSeries):
    provenance_columns = {"N.title": netflix.title, "N.type": netflix.type, "N.release_year": netflix.release_year, 
                         "N.age_certification": netflix.age_certification, "N.production_countries" : netflix.production_countries, 
                         "N.runtime": netflix.runtime,"N.seasons": netflix.seasons, "D.title": disney.title, "D.type": disney.type,
                         "D.release_year": disney.release_year, "D.rating" : disney.rating, "D.country": disney.country, 
                         "D.duration": disney.duration, "IMDBShows.Series_Title": imdbSeries.Series_Title, "IMDBShows.IMDB_Rating": imdbSeries.IMDB_Rating, 
                         "IMDBMovies.Series_Title": imdbMovies.Series_Title, "IMDBMovies.IMDB_Rating": imdbMovies.IMDB_Rating} 
                       
                       #   "D.title", "D.type", "D.release_year", "D.age_certification", "D.productionCountry", "D.duration", 
                       #   "IMDBShows.title", "IMDBShows.rating", "IMDBMovies.title", "IMDBMovies.rating", 
                       #   "TvShowsAndMoviesWithRating.title", "TvShowsAndMoviesWithRating.type", 
                       #   "TvShowsAndMoviesWithRating.release_year", "TvShowsAndMoviesWithRating.age_certification", 
                       #   "TvShowsAndMoviesWithRating.productionCountry", "TvShowsAndMoviesWithRating.runtime"]
    provenance = pd.DataFrame(provenance_columns)
    #provenance.columns = provenance_columns
    #provenance["N.title"] = netflix.title
    print("PROVENANCE!: ")
    print( provenance)

def run_etl():
    netflix, disney, imdbMovies, imdbSeries = extract()
    #build_provenance_table(netflix, disney, imdbMovies, imdbSeries)
    result = transform(netflix, disney, imdbMovies, imdbSeries)
    load(result)
