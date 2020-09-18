const fs = require('fs');
const express = require("express");
const cors = require("cors");

let port = process.env.PORT || 3000;

let data = JSON.parse(fs.readFileSync('./graph.json', {encoding:'utf8', flag:'r'}));
let labels = {}
let edges = {}
for (x of data.nodes) {
    labels[x.id] = x.label;
    edges[x.id] = [];
}
for (x of data.edges) edges[x.source].push(x.target);
console.info('Loaded graph')

let app = express();
app.use(cors());

app.get("/wikipedia-route/pages", (req,res) => {
    res.json(pages);
})

app.get("/wikipedia-route/:source/:target", (req, res) => {
    let {source, target} = req.params;
    console.info(`route query: ${source} ${target}`)
    res.json(["Australia", "United Kingdom", "Ireland"])
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});