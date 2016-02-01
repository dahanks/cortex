var main = function() {
    draw_audrey();
    draw_you();
    setup_websocket(location.host);
}

function draw_audrey() {
    $("#audrey").append("<div id=audrey-dialog class=dialog></div>");
    $("#audrey").append("<div id=audrey-avatar></div>");
}
function draw_you() {
    $("#you").append("<div id=you-dialog class=dialog contenteditable></div>");
    $("#you-dialog").keydown(function(e) {
        var key = e.which;
        if (key == 13) {
            submit_chat($(this).text());
        }
    });
}

function submit_chat(text_input) {
    $("#you-dialog").empty();
    submitAudreyChat(text_input);
}

function handle_audrey_response(message) {
    $("#audrey-dialog").text(message);
}

$(document).ready(main);
