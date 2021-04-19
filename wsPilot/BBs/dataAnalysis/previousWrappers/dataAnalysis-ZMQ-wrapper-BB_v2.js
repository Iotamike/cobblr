const zmq = require("zeromq")
var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');
var bent = require('bent');

//var app = express();
var lock = false;
//app.use(bodyParser.json());
async function runServer(){

	const sock = new zmq.Reply();

	var port = process.argv[2]; // port to talk to data Analysis block
	var newPort = process.argv[3]; // port to talk to another zmq wrapper

	await sock.bind("tcp://*: "+ newPort.toString());
	console.log("Data Analysis (responder) listening on localhost, port " +newPort.toString());

	

	for await (const [msg, body] of sock){

	   switch(msg.toString()){
	      case"/query":
		console.log(`FakeDB (${newPort}) received '${msg}'.`);
		console.log(msg.toString());
		console.log(body.toString());
		//console.log(targetJSON.toString());

		//targets = JSON.parse(targetJSON);
		//range = JSON.parse(rangeJSON);
                body_ = JSON.parse(body);
		//var ClientServerOptions = {
		//	uri: 'http://locahost:'+port.toString(),
		//	body: msg.toString()+targetsJson+adhocFiltersJson,
		//	method: 'POST',
		//	headers: {
		//	  'Content-Type':'application/json'
		//	}
		//}
		//
		//request(clientServerOptions, function(error, response){
		 //    console.log(error,response.body);
		//	return;
		//})
		//

		const post = bent('http://localhost:'+port.toString(),'POST','json',200);
		console.log(`forwarding request and waiting response`);		
		const response = await post(msg.toString(), body_);
		console.log(response);
		responseString = JSON.stringify(response);	

	       case "/search":

		body_ = JSON.parse(body);
		const post2 = bent('http://localhost:'+port.toString(),'POST','json', 200);
		const response2 = await post2(msg.toString(), body_);
		responseString = JSON.stringify(response2);
	     }

	sock.send(responseString);
	}
}

runServer()
