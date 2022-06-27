
$(document).ready(function(){

    //Nov 12 - don't need this now that checkboxes update the DB on click
    //select all checkbox
    // let select_all_checkboxes = document.getElementById("select_all_checkboxes");
    // select_all_checkboxes.addEventListener("click", function(){
    //     for(let i = 0; i < checkboxes.length; i++) {
    //         if(select_all_checkboxes.checked === true) {
    //             checkboxes[i].checked = true;
    //         }
    //         else {
    //             checkboxes[i].checked = false;
    //         }
    //     }
    // });

    //add an event listener to every checkbox
    let checkboxes = document.getElementsByClassName("individualCheckbox");
    for (let i = 0; i < checkboxes.length; i++) {
        checkboxes[i].addEventListener("change", updateObjectSelectionDB);
      }
    
    //Define separate function
    function updateObjectSelectionDB() {
        //post request to update ObjectSelection table in DB
        $.post( "/update_object_selection", 
            {"object_name": this.parentElement.parentElement.children[1].innerHTML
            ,"migrate": this.checked}
            // ,function(data, status, xhr) {   // success callback function
            //     alert('status: ' + status + ', data: ' + data.responseData);
            // }
            );

    }
});