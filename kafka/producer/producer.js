express = require('express');
var parser = require('body-parser');
var request = require('request');

var app = express();
var data ;
var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    client = new kafka.Client(),
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message')

app.use(parser.urlencoded({extended : false}));
app.use(parser.json());
 producer.on('ready', function () {
  console.log("Inside kafka producer")
  app.post('/',function(req,res){
   data = req.body;
   console.log(data); 
   payloads = [
    { topic: 'bollywoodkart', messages: JSON.stringify(data), partition: 0 }
   ];
   producer.send(payloads, function (err, data) {
     if (err) {
      console.log(err);
     }
     console.log(data);
     console.log('kafka');
   });
    res.send("Processed")
   });
 });
 producer.on('error', function(err) {
  console.log(err)
 });
app.listen(80);

