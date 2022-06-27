
$(document).ready(function(){

    //get buttons
    let sourcePullElements = document.getElementsByName("SourcePull");
    let destinationPushElements = document.getElementsByName("DestinationPush");

    //add event listeners
    for (let i = 0; i < sourcePullElements.length; i++) {
        sourcePullElements[i].addEventListener("click", pullFromSource);
      }
      for (let i = 0; i < destinationPushElements.length; i++) {
        destinationPushElements[i].addEventListener("click", pushToDestination);
      }
    

      function pullFromSource() {
        $.post("/pullFromSource",
        {
            "objectName": this.parentElement.parentElement.children[1].innerHTML
        }
        ,function(data, status){
            alert("Records pulled from source: " + data + "\nStatus: " + status)});
      }

      function pushToDestination() {
        $.post("pushToDestination",
        {
            "objectName": this.parentElement.parentElement.children[1].innerHTML
        });
      }



    
});