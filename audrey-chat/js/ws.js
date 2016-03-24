var client;
var url = "ws://" + location.host + ":61623";
var user = "admin";
var password = "password";
var wetware_topic = "/queue/wetware.nlp";
var reply_topics = [];
var reconnect_interval;

function setup_websocket() {
    if (window.WebSocket) {
        client = Stomp.client(url);
        client.connect(user, password, connect_callback, error_callback);
    } else {
        console.log("You're browser does not support WebSockets!");
    }
}

function on_message(message) {
    try {
        var dest = message['headers']['destination'];
        var body = JSON.parse(message['body']);
        if (dest.startsWith('/queue/temp')) {
            handle_audrey_response(body['responses']);
        } else {
            console.log("Not sure where this message came from...dropping...");
        }
    } catch (err) {
        console.log("Malformed message response");
    }
}

function connect_callback(frame) {
    clearInterval(reconnect_interval);
}

function error_callback(error) {
    if (error.indexOf("Stale connection") != -1){
        client.connect(user, password, connect_callback, error_callback);
    } else if (error.indexOf("Lost connection") != -1){
        reconnect_interval = setInterval(function() {
            client = Stomp.client(url);
            client.connect(user, password, connect_callback, error_callback);
        }, 5000);
    }
}

function submitAudreyChat(statement) {
    var json_data = {'statements': [statement]}
    var json_data_str = JSON.stringify(json_data);
    var reply_to_topic = '/temp-queue/' + guid();
    var headers = {'reply-to': reply_to_topic};
    reply_topics.push(reply_to_topic);
    client.subscribe(reply_to_topic, on_message);
    client.send(wetware_topic, headers, json_data_str);
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
