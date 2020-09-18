let express = require("express");
let cors = require("cors");
let app = express();

let port = process.env.PORT || 3000;

let pages = []
for (var i=0; i < 1000; i++) {
    pages.push(Math.random().toString(36).substring(7));
}

app.use(cors());

app.get("/wikipedia-route/pages", (req,res) => {
    res.json(pages);
})

app.get("/wikipedia-route/:source/:target", (req, res) => {
    res.json([req.params.source, req.params.target])
    
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
   });
   