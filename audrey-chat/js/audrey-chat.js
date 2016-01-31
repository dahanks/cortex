var main = function() {
    draw_modreq();
}

function draw_modreq() {
    draw_table();
}

function draw_table() {
    $("#bulk-modreq").append("<table id=modreq-table></table>");
}

$(document).ready(main);
