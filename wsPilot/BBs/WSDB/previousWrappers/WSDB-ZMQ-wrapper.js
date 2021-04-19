const zmq = require("zeromq")

async function runServer() {
    const sock = new zmq.Reply();

    // 'port' is the port used to communicate with data base service. 0MQ uses 'newPort'.
    var port = process.argv[2];
   // var newPort = parseInt(port)+1;
    newPort = process.argv[3];
    await sock.bind("tcp://*:"+newPort.toString()); 
    console.log("WSDB (responder) listening on localhost, port "+newPort.toString());

    var express = require('express');
    var bodyParser = require('body-parser');
    var _ = require('lodash');
    var app = express();

    app.use(bodyParser.json());

    var timeserie = require('./series');
    var countryTimeseries = require('./country-series');

    var now = Date.now();

    for (var i = timeserie.length - 1; i >= 0; i--) {
        var series = timeserie[i];
        var decreaser = 0;
        for (var y = series.datapoints.length - 1; y >= 0; y--) {
            series.datapoints[y][1] = Math.round((now - decreaser) / 1000) * 1000;
            decreaser += 50000;
        }
    }

    var annotation = {
        name: "annotation name",
        enabled: true,
        datasource: "generic datasource",
        showLine: true,
    }

    var annotations = [{
            annotation: annotation,
            "title": "Test message title",
            "time": 1450754160000,
            text: "teeext",
            tags: "taaags"
        },
        {
            annotation: annotation,
            "title": "Second test",
            "time": 1450754160000,
            text: "teeext",
            tags: "taaags"
        },
        {
            annotation: annotation,
            "title": "Third test",
            "time": 1450754160000,
            text: "teeext",
            tags: "taaags"
        }
    ];

    var tagKeys = [{
        "type": "string",
        "text": "Country"
    }];

    var countryTagValues = [{
            'text': 'SE'
        },
        {
            'text': 'DE'
        },
        {
            'text': 'US'
        }
    ];

    var now = Date.now();
    var decreaser = 0;
    for (var i = 0; i < annotations.length; i++) {
        var anon = annotations[i];

        anon.time = (now - decreaser);
        decreaser += 1000000
    }

    var table = {
        columns: [{
            text: 'Time',
            type: 'time'
        }, {
            text: 'Country',
            type: 'string'
        }, {
            text: 'Number',
            type: 'number'
        }],
        rows: [
            [1234567, 'SE', 123],
            [1234567, 'DE', 231],
            [1234567, 'US', 321],
        ]
    };

    var now = Date.now();
    var decreaser = 0;
    for (var i = 0; i < table.rows.length; i++) {
        var anon = table.rows[i];

        anon[0] = (now - decreaser);
        decreaser += 1000000
    }

    // Keep processing sockets-messages received from REQUESTER's ZeroMQ BB
    for await (const [msg, adhocFiltersJSON, targetsJSON] of sock) {

        console.log(`WSDB (${newPort}) received '${msg}'.`);
        var res = '';

	//switch (msg.toString()) {

         //   case "/":
         //       res = 'Success';
         //       break;

         //   case "/search":
         //       var result = [];
         //       _.each(timeserie, function(ts) {
          //          result.push(ts.target);
          //      });
         //       res = result;
         //       break;

         //   case "/annotations":
          //      res = annotations;
           //     break;

         //   case "/query":
          //      targets = JSON.parse(targetsJSON);
         //       adhocFilters = JSON.parse(adhocFiltersJSON);

         //       var tsResult = [];
         //       let fakeData = timeserie;

         //       if (adhocFilters && adhocFilters.length > 0) {
                    fakeData = countryTimeseries;
        //        }

          //      _.each(targets, function(target) {
         //           if (target.type === 'table') { 
         //               tsResult.push(table);
         //           } else {
        //                var k = _.filter(fakeData, function(t) {
         //                   return t.target === target.target; //target.target
        //                });

        //                _.each(k, function(kk) {
        //                    //console.log(kk);
         //                   tsResult.push(kk)
         //               });
        //            }
         //       });

        //        res = JSON.stringify(tsResult);
        //        break;

	    // The following two cases are NOT yet implemented/working
        //    case "/tag[\-]keys":
         //       res = tagKeys;
        //        break;
        //    case "/tag[\-]values":
         //       if (req.body.key == 'City') {
         //           res = cityTagValues;
         //       } else if (req.body.key == 'Country') {
        //            res = countryTagValues;
         //       }
         //       break;

         //   default:
        //        console.log(`No match!`);
        //        break;
      //  }

        console.log(`FakeDB (${newPort}) responding...`);
        sock.send(res); 
    }
}

function setCORSHeaders(res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "POST");
    res.setHeader("Access-Control-Allow-Headers", "accept, content-type");
}

runServer();
