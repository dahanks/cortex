var URL = "ws://" + location.host + ":61623";
var WETWARE_TOPIC = "/queue/wetware.nlp";

var partition;
var client;

function setup_websocket(username, password) {
    if (window.WebSocket) {
        /* This is intentionally insecure; we do not (yet) promise that users cannot
           access other users' partitions.  Authentication will be done elsewhere.
           Admin will have access to all partitions for the sake of demonstration. */
        if (username !== 'admin') {
            partition = username;
        }
        client = Stomp.client(URL);
        client.connect(username, password, null, function(error) {
            //go back to login on back connection/timeout
            location.reload();
        });
    } else {
        console.log("You're browser does not support WebSockets!");
    }
}

function on_message(message) {
    try {
        var dest = message['headers']['destination'];
        var body = JSON.parse(message['body']);
        if (dest.startsWith('/queue/temp')) {
            if (body['statements'] !== undefined) {
                handle_audrey_response(body['statements']);
            }
        } else {
            console.log("Not sure where this message came from...dropping...");
        }
    } catch (err) {
        console.log("Malformed message response");
    }
}

function publish_statement(statement, partition) {
    var json_data = {'statements': [statement]}
    if (partition) {
        json_data['partition'] = partition;
    }
    var json_data_str = JSON.stringify(json_data);
    var reply_to_topic = '/temp-queue/' + guid();
    var headers = {'reply-to': reply_to_topic};
    client.subscribe(reply_to_topic, on_message);
    client.send(WETWARE_TOPIC, headers, json_data_str);
}

function guid() {
  function s4() {
    return Math.floor((1 + Math.random()) * 0x10000)
      .toString(16)
      .substring(1);
  }
  return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
    s4() + '-' + s4() + s4() + s4();
}
