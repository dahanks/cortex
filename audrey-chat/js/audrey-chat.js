var main = function() {
    prompt_login();
}

function prompt_login() {
    $("#login_form").submit(function () {
        var username = $("#username").val();
        var password = $("#password").val();
        authenticate_user(username, password);
        //don't refresh after submit
        return false;
    });
}

function authenticate_user(username, password) {
    // test WS connection with credentials
    if (window.WebSocket) {
        client = Stomp.client(URL);
        client.connect(username, password, handle_auth_success, handle_auth_failure);

        // if success: tear it down and start over again
        function handle_auth_success(frame) {
            $(".prompt_box").hide();
            draw_audrey();
            draw_you();
            setup_websocket(username, password);
        }

        function handle_auth_failure(error) {
            $("#password").val('');
            $("#error_message").show();
        }
    } else {
        console.log("You're browser does not support WebSockets!");
    }
}

function draw_audrey() {
    $("#audrey").append("<div id=audrey-dialog class=dialog></div>");
    $("#audrey").append("<div id=audrey-avatar></div>");
    $("#audrey-avatar").append("<img id=audrey-img src='img/idle.gif'>");
    $("#audrey-dialog").text("Hello, I'm Audrey.");
}

function draw_you() {
    $("#you").append("<div id=you-avatar></div>");
    $("#you").append("<div id=you-dialog class=dialog contenteditable></div>");
    $("#you-avatar").append("<img id=you-img src='img/you.png'>");
    $("#you-dialog").keydown(function(e) {
        var key = e.which;
        if (key == 13) {
            submit_chat($(this).text());
        }
    });
}

function submit_chat(text_input) {
    $("#you-dialog").empty();
    $("#audrey-dialog").empty();
    publish_statement(text_input);
}

function handle_audrey_response(message) {
    var audrey_response = message[0];
    var audrey_reaction = "neutral";
    if (message.length > 1) {
        audrey_reaction = message[1];
    }
    $("#audrey-dialog").empty();
    $("#audrey-dialog").text(audrey_response);
    var img = document.getElementById("audrey-img");
    switch (audrey_reaction) {
    case "positive":
        img.src = 'img/yes.gif';
        setTimeout(function() {
            img.src = 'img/idle.gif';
        }, 2650);
        break;
    case "negative":
        img.src = 'img/no.gif';
        setTimeout(function() {
            img.src = 'img/idle.gif';
        }, 2800);
        break;
    case "neutral":
        img.src = 'img/neutral.gif';
        setTimeout(function() {
            img.src = 'img/idle.gif';
        }, 2400);
        break;
    default:
        img.src = 'img/neutral.gif';
        setTimeout(function() {
            img.src = 'img/idle.gif';
        }, 2400);
        break;
    }

    //somehow the you-dialog keeps getting a <br> at this point
    $("#you-dialog").empty();
}

$(document).ready(main);
