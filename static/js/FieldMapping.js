
$(document).ready(function(){

    //add an event listener to every select
    let selects = document.getElementsByClassName("fieldMapping");
    for (let i = 0; i < selects.length; i++) {
        selects[i].addEventListener("change", updateFieldMappingDB);
      }
    
    //Define separate function
    function updateFieldMappingDB() {
        //post request to update ObjectSelection table in DB
        $.post( "/update_field_mapping", 
            {"destination_field": this.options[this.selectedIndex].value
            ,"field_name": this.parentElement.parentElement.children[2].innerHTML
            ,"object_name": this.parentElement.parentElement.children[0].innerHTML}
            );

    }
});