const zmq = require("zeromq")

const socket = new zmq.Request();
var port = process.argv[2];
var newPort = process.argv[3];

// Nodejs listens to Grafana on port 'port'. 
// ZeroMQ communicates on 'newPort'.
socket.connect("tcp://localhost:"+newPort.toString());
console.log("Grafana-BB (requester) binding to localhost, port "+newPort.toString());

var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');
var app = express();
var lock = false;
app.use(bodyParser.json());

function setCORSHeaders(res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "POST");
    res.setHeader("Access-Control-Allow-Headers", "accept, content-type");
}

app.all('/', function(req, res) {

    // hacky 'semaphores'
    if (!lock)
        lock = true;
    else
        return;

    const msg = '/';
    console.log(`Grafana-BB (${newPort}) sending ${msg}`);
    var params = []
    params.push('/');
    params.push('');
    params.push('');

    socket.send(params);

    const msg2 = socket.receive();

    console.log(`Grafana-BB (${newPort}) received the following response: "${msg2}"`);
    setCORSHeaders(res);
    res.send(msg2);
    res.end();

    // hacky 'semaphores'
    lock = false;
});

app.all('/search', async function(req, res) {

    if (!lock)
        lock = true;
    else
        return;

    var params = []
    console.log(`Grafana-BB (${newPort}) is sending: /search`);
    params.push('/search');
    params.push(JSON.stringify(req.body));
    params.push('');
    await socket.send(params);

    const msg2 = await socket.receive();
    console.log(`Grafana-BB (${newPort}) received the following response: "${msg2}"`);
    //setCORSHeaders(res);
     res.setHeader("Context-Type", "application/json");
    //var reisult = []
   // let index = 0;
   // while (index < msg2.length) {
    //    result.push(msg2[index].toString());
   //     index += 1;
   // }
    console.log(JSON.parse(msg2));
    //res.json(result);
    res.send(JSON.parse(msg2));
    res.end();

    lock = false;
});

// Not yet implemented - is this needed?
app.all('/annotations', function(req, res) {

    if (!lock)
        lock = true;
    else
        return;

    console.log(`Grafana-BB (${newPort}) is sending: /annotations`);
    socket.send('/annotations');

    const msg2 = socket.receive();
    console.log(`Grafana-BB (${newPort}) received the following response: "${msg2}"`);
    setCORSHeaders(res);
    res.json(msg2);
    res.end();

    lock = false;
});

app.all('/query', async function(req, res) {

    console.log(`received request`);
    //if (!lock)
    //    lock = true;
    //else
    //    return;

    var params = []
    console.log(`Grafana-BB (${newPort}) is sending: /query`);
    params.push('/query');
    //console.log(req.body);
    params.push(JSON.stringify(req.body));
	//params.push(JSON.stringify(req.body.range));
    //params.push(JSON.stringify(req.body.targets));

    // Send message to FakeDB. We might be able to send the entire 'req'
    // object rather than just parts of it, but JSON.stringify cannot
    // directly work with 'req', as it has circular references.
    try{
	await socket.send(params);
    }catch(err){
      console.log('couldnt send');
      console.log(err);
    }
    // Wait for FakeDB to respond.
    const msg2 = await socket.receive();

    // The response is often too long; commented.
    //console.log(`Grafana-BB received the following response: "${msg2}"`);
    console.log(`Grafana-BB (${newPort}) received a response.`);
    setCORSHeaders(res);
    //console.log(res);	
    console.log(JSON.parse(msg2));
    res.send(JSON.parse(msg2));
    res.end();
    console.log(`sending msg to grafana`);
    lock = false;
});

app.listen(port);

console.log("Grafana's 0MQ BB is listening to Grafana on port "+port);
