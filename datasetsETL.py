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
    # Rename Disney columns to match Netflix, set up auxiliary structures
    disney.columns = ['title', 'type', 'description', 'release_year', 'age_certification', 'production_countries', 'runtime']
    available_on = []
    netflix["imdb_rating"] = "N/A"
    disney["imdb_rating"] = "N/A"

    # Auxiliary structure to check names
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
            # add imdb series score to netflix data set
            if row.title in imdbSeries[['Series_Title']].values:
                indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
                netflix.at[index, "imdb_rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
        else:
            netflix.at[index, 'type'] = 'Movie'
            netflix.at[index, 'runtime'] = str(row.runtime) + ' min'
            # add imdb movies score to netflix data set
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
        # add imdb series score to disney data set
        if row.type == 'TV Show':
            if row.title in imdbSeries[['Series_Title']].values:
                indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
                disney.at[index, "imdb_rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
        # add imdb movies score to disney data set
        else:
            if row.title in imdbMovies[['Series_Title']].values:
                indice = imdbMovies.index[imdbMovies['Series_Title'] == row.title].tolist()
                disney.at[index, "imdb_rating"] = imdbMovies.loc[indice[0], 'IMDB_Rating']

    netflix = netflix.assign(available_on=available_on).drop('seasons', axis=1)
    disney['available_on'] = 'Disney'
    result = pd.concat([netflix, disney], ignore_index=True)

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

def run_etl():
    netflix, disney, imdbMovies, imdbSeries = extract()
    result = transform(netflix, disney, imdbMovies, imdbSeries)
    load(result)
