"""
    This Flask web app provides a very simple dashboard to visualize the statistics sent by the spark app.
    The web app is listening on port 5000.
    All apps are designed to be run in Docker containers.
    
    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin and Joshua Rodrigues

"""

from cProfile import label
import sys
from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json
import datetime
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get('data')
    
    batch_count = 0
    batch_count +=1
    x_axis = []
    y_axis_py = []
    y_axis_php = []
    y_axis_ru = []
  
    try:
        data = json.loads(data)
    except TypeError:
        return "waiting for data..."

        
    try:
        py_index = data['Programming_Language'].index('Python')
        total_repo_py = data['Total_Repos'][py_index]
        stars_py = data['Total_stars'][py_index]
        y_axis_py.append(int(data['recent_repos'][py_index]))
        x_axis.append(datetime.now())
    except ValueError or IndexError:
        total_repo_py = 0
        stars_py = 0
        y_axis_py.append(int(0))
       
    
    try:
        ru_index = data['Programming_Language'].index('Ruby')
        total_repo_ru = data['Total_Repos'][ru_index]
        stars_ru = data['Total_stars'][ru_index]
        y_axis_ru.append(int(data['recent_repos'][ru_index]))
        
    except ValueError or IndexError:
        total_repo_ru = 0
        stars_ru = 0
        y_axis_ru.append(int(0))

    try:
        php_index = data['Programming_Language'].index('PHP')
        total_repo_php = data['Total_Repos'][php_index]
        stars_php = data['Total_stars'][php_index]
        y_axis_php.append(int(data['recent_repos'][php_index]))
        
    except ValueError or IndexError:
        total_repo_php = 0
        stars_php = 0
        y_axis_php.append(int(0))
    
    
    avg_stars_py = stars_py / (total_repo_py or 1)

    avg_stars_ru = stars_ru / (total_repo_ru or 1)

    avg_stars_php = stars_php / (total_repo_php or 1)


   


    f = plt.figure(1)
    plt.plot(x_axis, y_axis_py)
    # plt.plot(x_axis, y_axis_ru, color='blue', label='Ruby')
    # plt.plot(x_axis, y_axis_php, color='green', label='PHP')
    plt.legend(loc="upper left")
    plt.savefig('/streaming/webapp/static/images/lineg.png')
    
    


    g = plt.figure(2)
    x = [1, 2, 3]
    height = [avg_stars_py, avg_stars_ru, avg_stars_php]
    tick_label = ['total py repo','total Ruby repo', 'total php repo']
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:blue', 'tab:red' ])
    plt.ylabel('Average number of Stars')
    plt.savefig('/streaming/webapp/static/images/chart.png')
    return render_template('index.html', url='/static/images/chart.png', lgraph = '/static/images/lineg.png',  total_repo_py=total_repo_py, total_repo_ru = total_repo_ru,  total_repo_php=total_repo_php )



if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
