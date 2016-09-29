var client;
var URL = "ws://" + location.host + ":61623";
var REGISTER_USER = "registrar";
var REGISTER_QUEUE = "/queue/audrey.register";

var main = function() {
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
    client.send(REGISTER_QUEUE, null, json_data_str);
    window.location = "/audrey-chat";
}

function error_callback(error) {
    $("#error_message").show();
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
