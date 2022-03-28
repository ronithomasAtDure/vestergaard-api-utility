$('form[name="uploadForm"]').submit(function (event) {
    $('.toast').toast('show');
    document.getElementById("toast-body").innerHTML = "Please dont close the window. Fetching data in progess...";
});

$('form[name="dbuploadForm"]').submit(function (event) {
    $('.toast').toast('show');
    document.getElementById("toast-body").innerHTML = "Please dont close the window. Uploading to DB in progess...";
});

$('#password, #confirmPassword').on('keyup', function () {

    if ($('#password').val() == $('#confirmPassword').val()) {
        $('#message').html('Password Match').css('color', 'green');
        $("#userCreation").prop('disabled', false);
    } else if ($('#password').val() != $('#confirmPassword').val()) {
        $('#message').html('Password Dont Match').css('color', 'red');
        $("#userCreation").prop('disabled', true);
    }
});