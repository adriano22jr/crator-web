$(document).ready(function(){
    $("#continue-button").click(function(){
        var startUrlValue = document.getElementById("start-url").value;
        document.getElementById("hidden-start-url").value = startUrlValue;
    });
});