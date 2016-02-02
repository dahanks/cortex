var client;
var url = "ws://" + location.host + ":61623";
var user = "admin";
var password = "password";
var wetware_topic = "/topic/wetware.nlp";
var reply_topics = [];

// apollo_host can be set from the URL. Otherwise it defaults the server it is deployed on
// http://localhost:8888/ccd-web-git/beam_hlr_tabs.html?apollo_host=137.78.81.99
function setup_websocket(apollo_host) {
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
//    audrey_subscription = client.subscribe(audrey_topic, on_message);
}

function error_callback(error) {
    var json_data_str = JSON.stringify(error);
    console.log("Error: " + JSON.stringify(error));
    //TODO: verify that this works at all
    if (error['headers']) {
        var message = error['headers']['message'];
        console.log("Error: " + message);
        if (message.indexOf("Stale connection") != -1){
            client.connect(user, password, connect_callback, error_callback);
        }
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
