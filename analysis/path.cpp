#include <bits/stdc++.h>
using namespace std;

#define adjfile "../results/sawiki-20200401-links-adjlist.csv"

map<int,vector<int> > adj;

int seen[65000000], dist[65000000], p[65000000];

bool get_paths(int s, int e) {
    static int k = 0;
    k++;

    queue<int> q;
    q.push(s);
    seen[s] = k;
    dist[s] = 0;

    while (q.size() > 0 && seen[e] != k) {
        int t = q.front(); q.pop();
        for (int i : adj[t]) if (seen[i] != k) {
            seen[i] = k;
            dist[i] = dist[t]+1;
            p[i] = t;
            q.push(i);
        }
    }

    return seen[e] == k;
}

void print_path(int s, int e) {
    if (s != e) print_path(s, p[e]);
    printf("%d ", e);
}

int main() {
    FILE *f = fopen(adjfile, "r");
    int a,b;
    while (fscanf(f, " %d", &a) != EOF) {
        while (getc(f) == ' ') {
            fscanf(f, " %d", &b);
            adj[a].push_back(b);
        }
        if (adj.size() % 1000 == 0) printf("%lu\n", adj.size());
        fflush(stdout);
    }
    printf("%lu\n", adj.size());
    fflush(stdout);
    printf("ready\n");
    fflush(stdout);

    while (1) {
        scanf("%d%d", &a, &b);
        if (get_paths(a,b)) print_path(a,b), printf("\n");
        else printf("no path\n");
        fflush(stdout);
        break;
    }
}
