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
    provenance_columns = ["N_title", "N_type", "N_description", "N_release_year", "N_age_certification", "N_production_countries", 
    "N_runtime", "N_seasons", "D_title", "D_type", "D_description","D_release_year", "D_rating", "D_country", "D_duration", 
    "IMDBShows_Series_Title", "IMDBShows_IMDB_Rating", "IMDBMovies.IMDB_Rating","IMDBMovies.Series_Title", 
    "TvShowsAndMoviesWithRating_title", "TvShowsAndMoviesWithRating_type", "TvShowsAndMoviesWithRating_description", "TvShowsAndMoviesWithRating_release_year", 
    "TvShowsAndMoviesWithRating_age_certification", "TvShowsAndMoviesWithRating_productionCountry", 
    "TvShowsAndMoviesWithRating_runtime"]
  #  provenance_columns = {"N_title": netflix.title, "N_type": netflix.type, "N_release_year": netflix.release_year, 
  #                       "N_age_certification": netflix.age_certification, "N_production_countries" : netflix.production_countries, 
  #                      "N_runtime": netflix.runtime,"N_seasons": netflix.seasons, "D_title": disney.title, "D_type": disney.type,
  #                       "D_release_year": disney.release_year, "D_rating" : disney.rating, "D_country": disney.country, 
  #                       "D_duration": disney.duration, "IMDBShows.Series_Title": imdbSeries.Series_Title, "IMDBShows.IMDB_Rating": imdbSeries.IMDB_Rating, 
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
        provenance.at[index, "N_title"] = row.title
        provenance.at[index, "N_description"] = row.description
        provenance.at[index, "N_production_countries"] = row.production_countries
        provenance.at[index, "N_type"] = row.type
        provenance.at[index, "N_release_year"] = row.release_year
        provenance.at[index, "N_age_certification"] = row.age_certification
        provenance.at[index, "N_runtime"] = row.runtime
        provenance.at[index, "N_seasons"] = row.seasons
        provenance.at[index, "TvShowsAndMoviesWithRating_title"] = row.title
        provenance.at[index, "TvShowsAndMoviesWithRating_release_year"] = row.release_year
        provenance.at[index, "TvShowsAndMoviesWithRating_age_certification"] = row.age_certification
        provenance.at[index, "TvShowsAndMoviesWithRating_description"] = row.description

        provenance.at[index, "TvShowsAndMoviesWithRating_production_countries"] = countries.netflix_to_disney(row.production_countries)
        if row.type == 'SHOW':
            provenance.at[index, "TvShowsAndMoviesWithRating_type"] = 'TV Show'

            if row.seasons == 1:
                netflix.at[index, 'runtime'] = str(int(row.seasons)) + ' Season'
            else:
                netflix.at[index, 'runtime'] = str(int(row.seasons)) + ' Seasons'
            # add imdb series score to netflix data set
            if row.title in imdbSeries[['Series_Title']].values:
                indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
                provenance.at[index, "IMDBShows_Series_Title"] = row.title
                provenance.at[index, "IMDBShows_IMDB_Rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
        else:
            provenance.at[index, "TvShowsAndMoviesWithRating_type"] = "Movie"
            provenance.at[index, "TvShowsAndMoviesWithRating_runtime"] = str(row.runtime) + ' min'
            # add imdb movies score to netflix data set
            if row.title in imdbMovies[['Series_Title']].values:
                indice = imdbMovies.index[imdbMovies['Series_Title'] == row.title].tolist()
                provenance.at[index, "IMDBMovies_Series_Title"] = row.title
                provenance.at[index, "IMDBMovies_IMDB_Rating"] = imdbMovies.loc[indice[0], 'IMDB_Rating']

        if str(row.title).lower() in disney_titles_only['title'].values:
            available_on.append("Netflix, Disney+")
            indice = disney_titles_only.index[disney_titles_only['title'] == str(row.title).lower()].tolist()
            provenance.at[index, "D_title"] = disney.loc[indice[0], 'title']
            provenance.at[index, "D_type"] = disney.loc[indice[0], 'type']
            provenance.at[index, "D_description"] = disney.loc[indice[0], 'description']
            provenance.at[index, "D_release_year"] = disney.loc[indice[0], 'release_year']
            provenance.at[index, "D_rating"] = disney.loc[indice[0], 'rating']
            provenance.at[index, "D_country"] = disney.loc[indice[0], 'country']
            provenance.at[index, "D_duration"] = disney.loc[indice[0], 'duration']
            provenance.at[index, "TvShowsAndMoviesWithRating_available_on"] = "Netflix, Disney+"
            disney.drop(index=indice[0], inplace=True)
        else:
            provenance.at[index, "TvShowsAndMoviesWithRating_available_on"] = "Netflix"
            available_on.append("Netflix")

    for index, row in disney.iterrows():

        provenance2.at[index, "D_title"] = row.title
        provenance2.at[index, "D_type"] = row.type
        provenance2.at[index, "D_description"] = row.description
        provenance2.at[index, "D_release_year"] = row.release_year
        provenance2.at[index, "D_rating"] = row.rating
        provenance2.at[index, "D_country"] = row.country
        provenance2.at[index, "D_duration"] = row.duration
        provenance2.at[index, "TvShowsAndMoviesWithRating_title"] = row.title
        provenance2.at[index, "TvShowsAndMoviesWithRating_type"] = row.type
        provenance2.at[index, "TvShowsAndMoviesWithRating_description"] = row.description
        provenance2.at[index, "TvShowsAndMoviesWithRating_release_year"] = row.release_year
        provenance2.at[index, "TvShowsAndMoviesWithRating_age_certification"] = row.rating
        provenance2.at[index, "TvShowsAndMoviesWithRating_production_countries"] = row.country
        provenance2.at[index, "TvShowsAndMoviesWithRating_runtime"] = row.duration
        # add imdb series score to disney data set
        if row.type == 'TV Show':
            if row.title in imdbSeries[['Series_Title']].values:
                indice = imdbSeries.index[imdbSeries['Series_Title'] == row.title].tolist()
                provenance2.at[index, "IMDBShows_Series_Title"] = row.title
                provenance2.at[index, "IMDBShows_IMDB_Rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
                provenance2.at[index, "TvShowsAndMoviesWithRating_imdb_rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
        # add imdb movies score to disney data set
        else:
            if row.title in imdbMovies[['Series_Title']].values:
                indice = imdbMovies.index[imdbMovies['Series_Title'] == row.title].tolist()
                provenance2.at[index, "IMDBMovies_Series_Title"] = row.title
                provenance2.at[index, "IMDBMovies_IMDB_Rating"] = imdbSeries.loc[indice[0], 'IMDB_Rating']
                provenance2.at[index, "TvShowsAndMoviesWithRating_imdb_rating"] = imdbMovies.loc[indice[0], 'IMDB_Rating']

    netflix = netflix.assign(available_on=available_on).drop('seasons', axis=1)
    provenance2['TvShowsAndMoviesWithRating_available_on'] = 'Disney+'
    provenance_result = pd.concat([provenance, provenance2], ignore_index=True)      

    result = provenance_result [['TvShowsAndMoviesWithRating_title', 'TvShowsAndMoviesWithRating_type','TvShowsAndMoviesWithRating_description',
                        'TvShowsAndMoviesWithRating_release_year', 'TvShowsAndMoviesWithRating_age_certification', 
                       'TvShowsAndMoviesWithRating_production_countries', 'TvShowsAndMoviesWithRating_runtime', 
                       'TvShowsAndMoviesWithRating_imdb_rating',
                       'TvShowsAndMoviesWithRating_available_on']]
    print("PROVENANCE!: ")
   
    result.columns = ['title', 'type', 'description', 'release_year', 'age_certification', 'production_countries', 'runtime', 'imdb_rating', 'available_on']  
    result = result.fillna(value="")
    provenance_result = provenance_result.fillna(value="")
    print(result)
    return result, provenance_result

def load(result, provenance):
    print("Loading...")
    build_provenance_table(provenance)
    database_enabled = config.get('DatabaseSection', 'use.database') == 'True'
    if database_enabled:
        dbname = config.get('DatabaseSection', 'database.dbname')
        conn = connection.set_up_conn()
        cursor = conn.cursor()
        cursor.execute(f""" IF OBJECT_ID(N'{dbname}.dbo.TvShowsAndMoviesWithRating', N'U') IS NOT NULL  
                      DROP TABLE {dbname}.dbo.TvShowsAndMoviesWithRating""")
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


def build_provenance_table(result):
    database_enabled = config.get('DatabaseSection', 'use.database') == 'True'
    if database_enabled:
        dbname = config.get('DatabaseSection', 'database.dbname')
        conn = connection.set_up_conn()
        cursor = conn.cursor()
        cursor.execute(f""" IF OBJECT_ID(N'{dbname}.dbo.Provenance', N'U') IS NOT NULL  
                        DROP TABLE {dbname}.dbo.Provenance""")
        conn.commit()
        cursor.execute(f"""CREATE TABLE Provenance (
                            N_title VARCHAR(200),
                            N_description VARCHAR(2000),
                            N_type VARCHAR(10),
                            N_release_year VARCHAR(10),
                            N_age_certification VARCHAR(50),
                            N_production_countries VARCHAR(500),
                            N_runtime VARCHAR(50),
                            N_seasons VARCHAR(10),
                            D_title VARCHAR(200),
                            D_type VARCHAR(10),
                            D_description VARCHAR(2000),
                            D_release_year VARCHAR(10),
                            D_rating VARCHAR(50),
                            D_country VARCHAR(500),
                            D_duration VARCHAR(50),
                            IMDBShows_Series_Title VARCHAR(200), 
                            IMDBShows_IMDB_Rating VARCHAR(10),
                            IMDBMovies_Series_Title VARCHAR(200), 
                            IMDBMovies_IMDB_Rating VARCHAR(10), 
                            TvShowsAndMoviesWithRating_title VARCHAR(200),
                            TvShowsAndMoviesWithRating_type VARCHAR(10),
                            TvShowsAndMoviesWithRating_description VARCHAR(2000),
                            TvShowsAndMoviesWithRating_release_year VARCHAR(10),
                            TvShowsAndMoviesWithRating_age_certification VARCHAR(50),
                            TvShowsAndMoviesWithRating_production_countries VARCHAR(500),
                            TvShowsAndMoviesWithRating_runtime VARCHAR(50),
                            TvShowsAndMoviesWithRating_imdb_rating VARCHAR(10),
                            TvShowsAndMoviesWithRating_available_on VARCHAR(50)
                            )
                            """)
        conn.commit()
        for index, row in result.iterrows():
            cursor.execute(f"""INSERT INTO {dbname}.dbo.Provenance (N_title,
                            N_type,
                            N_description,
                            N_release_year,
                            N_age_certification,
                            N_production_countries,
                            N_runtime,
                            N_seasons,
                            D_title,
                            D_type,
                            D_description,
                            D_release_year,
                            D_rating,
                            D_country,
                            D_duration,
                            IMDBShows_Series_Title, 
                            IMDBShows_IMDB_Rating,
                            IMDBMovies_Series_Title, 
                            IMDBMovies_IMDB_Rating, 
                            TvShowsAndMoviesWithRating_title,
                            TvShowsAndMoviesWithRating_type,
                            TvShowsAndMoviesWithRating_description,
                            TvShowsAndMoviesWithRating_release_year,
                            TvShowsAndMoviesWithRating_age_certification,
                            TvShowsAndMoviesWithRating_production_countries,
                            TvShowsAndMoviesWithRating_runtime,
                            TvShowsAndMoviesWithRating_imdb_rating,
                            TvShowsAndMoviesWithRating_available_on) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            row['N_title'], row['N_type'], row['N_description'], row['N_release_year'], row['N_age_certification'],                     
                            row['N_production_countries'], row['N_runtime'], row['N_seasons'], row['D_title'], 
                            row['D_type'], row['D_description'], str(row['D_release_year']), row['D_rating'],
                            row['D_country'], row['D_duration'], 
                            row['IMDBShows_Series_Title'], row['IMDBShows_IMDB_Rating'],
                            row['IMDBMovies_Series_Title'], row['IMDBMovies_IMDB_Rating'],
                            row['TvShowsAndMoviesWithRating_title'], 
                            row['TvShowsAndMoviesWithRating_type'], row['TvShowsAndMoviesWithRating_description'], 
                            row['TvShowsAndMoviesWithRating_release_year'], 
                            row['TvShowsAndMoviesWithRating_age_certification'],
                            row['TvShowsAndMoviesWithRating_production_countries'], row['TvShowsAndMoviesWithRating_runtime'],
                            row['TvShowsAndMoviesWithRating_imdb_rating'], row['TvShowsAndMoviesWithRating_available_on'])
        conn.commit()
        cursor.close()
    else:
        result.to_csv('datos/provenance.csv')

def run_etl():
    netflix, disney, imdbMovies, imdbSeries = extract()
    result, provenance = transform(netflix, disney, imdbMovies, imdbSeries)
    load(result, provenance)
