let express = require("express");
let app = express();

let port = process.env.PORT || 3000;

app.get("/api/wikipedia-route/:source/:target", (req, res) => {
    res.json([req.params.source, req.params.target])
    
});

app.listen(port, () => {
    console.log("Server running on port 3000");
   });
   