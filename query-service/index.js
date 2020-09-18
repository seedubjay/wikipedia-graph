var express = require("express");
var app = express();

app.get("/api/wikipedia-route/:source/:target", (req, res) => {
    res.json([req.params.source, req.params.target])
    
});

app.listen(3000, () => {
    console.log("Server running on port 3000");
   });
   