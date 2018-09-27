var fs              = require('fs');
var WebSocketServer = require('websocket').server;
var express         = require('express')
       , bodyParser = require('body-parser');
var app             = express();

var server = app.listen(8080, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("RESTful API and WebSockets listening at http://%s:%s", host, port)

})
var wsServer        = new WebSocketServer({ httpServer : server });

app.use(bodyParser.json());

clients=[];
nodes=[];

function remove_nodes() {
  var now = new Date();
  for (i in nodes) {
   var timeDiff = now-nodes[i].last;
   if (timeDiff/1000 > 2) {
     console.log("Node " + nodes[i].name + " stopped transmitting. Removing");
     nodes.splice(i, 1);
   }
  }
}

function find_node(name) {
  for (i in nodes) {
    if (nodes[i].name == name) {
      return i; 
    }
  }
  return -1; 
}

setInterval(function(){remove_nodes();}, 1000);

app.post('/rest/', function (req, res) {
   var s = req.url.split('?',2);
   var node_name = 'default';

   if (nodes.length>1) {
    node_name = nodes[0].name;
   }
   if (s.length == 2) {
    node_name = s[1];
   }

   console.log("RECV: node=" + node_name);
   
   var node_pos = find_node(node_name);
   if (node_pos == -1) {
     fs.writeFile("airscope_node_" + node_name, "Starting record\n", function(err) {
       if(err) {
         console.log(err);
       }
     });
     var data = req.body;
     for (i in data) {
       if (data[i].name == 'side_info') {
         node_desc = data[i].title;
         var n = {name: node_name, desc: node_desc, last: new Date()};
         nodes.push(n);
         console.log('Added new node ' + node_name + ' (' + node_desc + ')'); 
       }
     }
   } else {
     nodes[node_pos].last = new Date();
     fs.appendFile("airscope_node_" + node_name, JSON.stringify(req.body), function(err) {
       if(err) {
         console.log(err);
       }
     });
   }

   for (var i=0;i<clients.length;i++) {
    if (clients[i].node == node_name) {
     clients[i].conn.sendUTF(JSON.stringify(req.body));
     console.log("SEND: id=", + i + ", node=" + node_name);
    }
   }
   res.sendStatus(200)
})

function originIsAllowed(origin) {
  // put logic here to detect whether the specified origin is allowed.
  return origin == "http://34.249.41.170";
}

wsServer.on('request', function(request) {
  if (!originIsAllowed(request.origin)) {
    // Make sure we only accept requests from an allowed origin
    request.reject();
    console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
    return;
  }
  
  var connection = request.accept('ascope',request.origin);

  console.log((new Date()) + ' Connection accepted.');
  connection.on('message', function(message) {
    if (message.type === 'utf8') {
      console.log('Received Message: ' + message.utf8Data);
      
      // client request to display a node
      clients.push({node: message.utf8Data, conn: connection});
      // send him the rest of registered nodes 
      connection.sendUTF(JSON.stringify(nodes));
    }
  });
  connection.on('close', function(reasonCode, description) {
    for (i in clients) {
      if (clients[i].conn == connection) {
        clients.splice(i,1);
      }
    }
    console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.');
  });
});
