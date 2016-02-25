var main = function() {
    draw_audrey();
    draw_you();
    setup_websocket();
}

function draw_audrey() {
    $("#audrey").append("<div id=audrey-dialog class=dialog></div>");
    $("#audrey").append("<div id=audrey-avatar></div>");
    $("#audrey-avatar").append("<img id=audrey-img src='img/idle.gif'>");
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
    submitAudreyChat(text_input);
}

function handle_audrey_response(message) {
    $("#audrey-dialog").empty();
    $("#audrey-dialog").text(message);
    var img = document.getElementById("audrey-img");
    if (message.indexOf("Yes,") > -1) {
        img.src = 'img/yes.gif';
        setTimeout(function() {
            img.src = 'img/idle.gif';
        }, 2650);
    } else if (message.indexOf("No,") > -1) {
        img.src = 'img/no.gif';
        setTimeout(function() {
            img.src = 'img/idle.gif';
        }, 2800);
    } else {
        img.src = 'img/neutral.gif';
        setTimeout(function() {
            img.src = 'img/idle.gif';
        }, 2400);
    }
}

$(document).ready(main);
