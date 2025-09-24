$(document).ready(function(){
    $("#continue-button").click(function(){
        var startUrlValue = document.getElementById("start-url").value;
        document.getElementById("hidden-start-url").value = startUrlValue;
    });
});

$(document).ready(function(){
    $("#btn-marketplaces").click(function(){
        $("#marketplaces-card").show();
        $("#crawl-card").hide();
    });

    $("#btn-crawl").click(function(){
        $("#crawl-card").show();
        $("#marketplaces-card").hide();
    });
});

$(document).ready(function(){
    $("#switch-has-cookie").change(function(){
        const isChecked = $(this).prop('checked');
        $('#has-cookie-value').val(isChecked);
    });
});

$(document).ready(function() {
    $('#crawlTable').DataTable({
        "pageLength": 25,      
        "order": [[4, "desc"]],
        "lengthMenu": [10, 25, 50, 100],
        "columnDefs": [
            { "orderable": false, "targets": [] }
        ], 
        "searching": false
    });
});