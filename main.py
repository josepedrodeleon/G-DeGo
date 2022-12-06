from flask import *
import json, time 
import datasetsETL as dataset
app = Flask(__name__)



@app.route('/whereToWatch/<contentname>')
def where_to_watch(contentname):
  #returns the username
  try :
    available_on = dataset.where_to_watch(contentname)
    data_set = {'AvailableOn': f'{available_on}'}
  except:
    data_set = {'AvailableOn': 'The movie or show is not available in Disney+ or Netflix'}
  json_dump = json.dumps(data_set)
  return json_dump

@app.route('/movieOrShowInfo/<contentname>')
def content_information(contentname):
    content_info = dataset.get_content_info(contentname)
    return content_info

@app.route('/moviesAndShows')
def movies_and_shows():
    result = dataset.get_movies_and_shows()    
    return render_template('movies_and_shows.html', your_list=result)

@app.route('/producedInUruguay')
def produced_in_uruguay():
    result = dataset.produced_in_uruguay()
    return result
    
if __name__ == '__main__':
    app.run(port=7777)