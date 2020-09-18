const fs = require('fs');
const express = require("express");
const cors = require("cors");

let port = process.env.PORT || 3000;

let data = JSON.parse(fs.readFileSync('./graph.json', {encoding:'utf8', flag:'r'}));
let labels = new Map()
let edges = new Map()
for (x of data.nodes) {
    labels.set(x.id, x.label);
    edges.set(x.id, []);
}
for (x of data.edges) edges.get(x.source).push(x.target);
console.info('Loaded graph')

function dfs(node, depth, path, target) {
    path.push(node)
    console.log(path);
    console.log(edges.get(node));
    if (node === target) return path;
    if (depth) {
        for (e of edges.get(node)) {
            let a = dfs(e, depth-1, path, target);
            if (a) return a;
        }
    }
    path.pop(node);
    return null;
}

function deepening_path(source, target) {
    for (let i = 0; i < 50; i++) {
        let p = dfs(source, i, [], target);
        if (p) return p.map(i => labels.get(i));
    }
    return null;
}

function path(source, target) {
    let dist = new Map()
    let prev = new Map()
    dist.set(source, 0);
    let active = [source];
    let activeStart = 0;
    while (activeStart < active.length && !dist.has(target)) {
        let i = active[activeStart++];
        for (j of edges.get(i)) {
            if (!dist.has(j)) {
                dist.set(j, dist.get(i) + 1);
                prev.set(j, i);
                active.push(j);
            }
        }
    }

    let soln = [target];
    while (soln[soln.length-1] != source) {
        soln.push(prev.get(soln[soln.length-1]));
    }

    return soln.map(i => labels.get(i)).reverse();
}

console.log(deepening_path(65546, 32897));

let app = express();
app.use(cors());

app.get("/wikipedia-route/pages", (req,res) => {
    res.json(pages);
})

app.get("/wikipedia-route/:source/:target", (req, res) => {
    let {source, target} = req.params;
    console.info(`route query: ${source} ${target}`)
    res.json(deepening_path(source,target))
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});