const fs = require('fs');
const express = require("express");
const cors = require("cors");

let port = process.env.PORT || 3000;

let app = express();
app.use(cors());

let data = JSON.parse(fs.readFileSync('./graph.json', {encoding:'utf8', flag:'r'}));
app.locals.pages = data.nodes
app.locals.labels = new Map()
app.locals.edges = new Map()
for (x of data.nodes) {
    app.locals.labels.set(x.id, x.label);
    app.locals.edges.set(x.id, []);
}
for (x of data.edges) app.locals.edges.get(x[0]).push(x[1]);
console.info('Loaded graph')

function dfs(node, depth, path, target) {
    path.push(node)
    if (node === target) return path;
    if (depth) {
        for (e of app.locals.edges.get(node)) {
            let a = dfs(e, depth-1, path, target);
            if (a) return a;
        }
    }
    path.pop(node);
    return null;
}

function path(source, target) {
    for (let i = 0; i < 20; i++) {
        let p = dfs(source, i, [], target);
        if (p) return p.map(i => app.locals.labels.get(i));
    }
    return null;
}

app.get("/pages", (req,res) => {
    res.json(app.locals.pages);
})

app.get("/path/:source/:target", (req, res) => {
    let {source, target} = req.params;
    source = parseInt(source);
    target = parseInt(target)
    console.info(`route query: ${source} ${target}`)
    res.json(path(source,target))
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});