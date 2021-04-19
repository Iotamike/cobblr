from flask import Flask, Response, abort, request
from flask import request, jsonify, json
from flask_cors import CORS, cross_origin
import datetime
import pandas as pd
import numpy as np
import requests

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

methods = ('GET', 'POST')

htmlAddress = "https://admin.moninamo.com/api/v1/devices/A4CF1296B65C/stats?since="

@app.route('/getWSData', methods=methods)
@cross_origin()
def getWSData():

    
    req = request.get_json()
    startDate = req['startDate']
    rawendDate = req['rawEndDate']
    endTimeStamp = req['endDate'] 

    #startTimeStamp = startDate.timestamp()
    #endTimeStamp = endDate.timestamp()

    #startTimeStamp = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')

    endDateReached = 0
    #hA = htmlAddress + str(int(startTimeStamp))
    hA = htmlAddress + startDate
    data = pd.DataFrame()
    print(hA)

    while hA is not None and endDateReached == 0:
        resp = requests.get(url=hA)
        results = resp.json()

        temp = pd.io.json.json_normalize(results['results'])



        if (temp.size > 0):

            finalDate = temp['sampled_at'].iloc[-1]
            print(finalDate)

            if (float(endTimeStamp) > datetime.datetime.strptime(finalDate, "%Y-%m-%d %H:%M:%S").timestamp()):
                endDateReached = 0
            else:
                endDateReached = 1
            data = data.append(temp, ignore_index=True)

        #print(data)
        hA = results['links']['next']
        print("next: ",hA)

    if data.size >  0:
        data = data[pd.to_datetime(data['sampled_at'], utc=True) < pd.to_datetime(rawendDate)]
        data = data.to_json(orient="split")
    else:
        data = "{}"
    print(data)
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
