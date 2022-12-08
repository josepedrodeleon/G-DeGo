from flask import *
import json, time 
import datasetsETL as dataset
import queryModule
import configparser

app = Flask(__name__)
config = configparser.RawConfigParser()
config.read('application.properties')

@app.route('/whereToWatch/<contentname>')
def where_to_watch(contentname):
  #returns the username
  try :
    available_on = queryModule.where_to_watch(contentname)
    data_set = {'AvailableOn': f'{available_on}'}
  except:
    data_set = {'AvailableOn': 'The movie or show is not available in Disney+ or Netflix'}
  json_dump = json.dumps(data_set)
  return json_dump

@app.route('/movieOrShowInfo/<contentname>')
def content_information(contentname):
    content_info = queryModule.get_content_info(contentname)
    return content_info

@app.route('/moviesAndShows')
def movies_and_shows():
    result = queryModule.get_movies_and_shows()
    return render_template('movies_and_shows.html', your_list=result)

@app.route('/producedInUruguay')
def produced_in_uruguay():
    result = queryModule.produced_in_uruguay()
    return result
    
if __name__ == '__main__':
    if str(input("Run ETL? (yes/no)\n")).lower() == "yes":
        print("Running ETL, please wait...")
        dataset.run_etl()
    else:
        print("Not running ETL.")
    print("Starting app...")
    app.run(port=7777)
