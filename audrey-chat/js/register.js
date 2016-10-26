var client;
var URL = "ws://" + location.host + ":61623";
var REGISTER_USER = "registrar";
var REGISTER_QUEUE = "/queue/audrey.register";

var main = function() {
    $("#username").keyup(function() {
        var username = $(this).val();
        $("#mirror_username").html(username || '&nbsp');
    });
    $("#password").change(validate_passwords);
    $("#confirm_password").change(validate_passwords);
    $("#register_form").submit(function () {
        register_audrey();
        //don't refresh after submit
        return false;
    });
}

function register_audrey() {
    var auth = $("#auth_code").val();
    if (window.WebSocket) {
        client = Stomp.client(URL);
        client.connect(REGISTER_USER, auth, connect_callback, error_callback);
    } else {
        console.log("You're browser does not support WebSockets!");
    }
}

function connect_callback(frame) {
    $("#error_message").hide();
    var user = $("#username").val();
    var conf = $("#confirm_password").val();
    var json_data = {
        'username': user,
        'password': conf
    }
    var json_data_str = JSON.stringify(json_data);
    var reply_to_topic = '/temp-queue/' + user;
    client.subscribe(reply_to_topic, on_message);
    client.send(REGISTER_QUEUE, {'reply-to': reply_to_topic}, json_data_str);

    function on_message(message) {
        try {
            var body = JSON.parse(message['body']);
            if (body.error !== undefined) {
                $("#error_message").html(body.error).show();
            } else {
                window.location = "/audrey-chat";
            }
        } catch (err) {
            console.log("Malformed message response");
        }
    }
}

function error_callback(error) {
    $("#auth_code").val('');
    $("#error_message").html("Invalid authentication code").show();
}

function validate_passwords() {
    var password = document.getElementById("password");
    var confirm_password = document.getElementById("confirm_password");
    if (password.value !== confirm_password.value) {
        confirm_password.setCustomValidity("Passwords don't match");
    } else {
        confirm_password.setCustomValidity("");
    }
}

$(document).ready(main);
