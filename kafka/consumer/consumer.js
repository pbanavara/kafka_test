var client = process.argv[2]

if (client == "mongo") {
 let client = require('mongodb').MongoClient;
 var collection;
 client.connect('mongodb://139.59.67.134:27000/triggers', function (err, db){
  if (err) { console.log('db error'); }
  console.log('db connected');
  collection = db.collection('triggers'); 
 });
}

if (client == "cassandra") {
 const cassandra = require('cassandra-driver');
 let client = new cassandra.Client({ contactPoints: ['139.59.80.4'] , keyspace : 'events'});
 client.connect(function (err) {
   console.log("Cassandra Error ::: " + err);
 });
}

var kafka = require('kafka-node'),
  HighLevelConsumer = kafka.HighLevelConsumer,
  Consumer = kafka.Consumer,
  kafkaClient = new kafka.Client(),
  consumer = new HighLevelConsumer(
    kafkaClient,
    [
      { topic: 'bollywoodkart', partition: 0 }
    ],
    {
      autoCommit: false,
      fromOffset: false
    }
  );
consumer.on('message', function (message) {
    console.log("Message is ::: " + message);
   let data = JSON.parse(message.value);
  console.log(data);
  data = data.Data;
  if (client == "mongo") {
   collection.insert(data,function (err) {
    if (err) { console.log(err)}
    else{
     console.log("inserted");
    }
       
   });
  }
  if (client == "cassandra") {
   pushDataToCassandra(message, function (success, error) {
    console.log(success);
   });
  }
});

function pushDataToCassandra(message, callback) {
 var obj = JSON.parse(message.value);
 var sendObj = obj.Data;
 const query = "INSERT into event JSON'" + JSON.stringify(sendObj) + "'";
 client.execute(query, function (err, result) {
  if( ! err) {
   console.log("Inserted");
   } else {
   console.log(err);
   }
  callback(result, err);
 });
}
consumer.on('error', function (err) {
        console.log(err);
});

