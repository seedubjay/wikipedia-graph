#include <bits/stdc++.h>
using namespace std;

using ll = long long;

const char *adjfile = "results/enwiki-20201220-links-adjlist.csv";

const int MAXN = 6500000;

int N;

int pid_to_cid[70000000];
int cid_to_pid[MAXN];
vector<int> adj[MAXN];

double betweenness[MAXN];
int dist[MAXN];
ll numpaths[MAXN];
vector<int> pred[MAXN];
int bfs[MAXN];

void go(int start) {
    for (int i = 0; i < N; i++) {
        betweenness[i] = 0;
        dist[i] = 1e9;
        numpaths[i] = 0;
        pred[i].clear();
    }

    bfs[0] = start;
    dist[start] = 0;
    numpaths[start] = 1;
    int s = 0, e = 1;
    while (s < e) {
        int v = bfs[s++];
        for (int w : adj[v]) {
            if (dist[w] == 1e9) {
                dist[w] = dist[v]+1;
                bfs[e++] = w;
            }
            if (dist[w] == dist[v] + 1) {
                numpaths[w] += numpaths[v];
                pred[w].push_back(v);
            }
        }
    }

    for (int i = s-1; i >= 0; i--) {
        int w = bfs[i];
        for (int v : pred[w]) {
            betweenness[v] += (double)numpaths[v] / (double)numpaths[w] * (1.0 + betweenness[w]);
        }
    }
}

double temp[MAXN];
double total_betweenness[MAXN];
double total_betweenness_denom[MAXN];
int ordering[MAXN];

int main() {
    FILE *f = fopen(adjfile, "r");
    int a,b;
    int maxpid = 0;
    while (fscanf(f, " %d", &a) != EOF) {
        if (a > maxpid) maxpid = a;
        pid_to_cid[a] = N;
        cid_to_pid[N] = a;
        while (getc(f) == ' ') {
            fscanf(f, " %d", &b);
            adj[N].push_back(b);
        }
        ordering[N] = N;
        N++;
        if (N % 10000 == 0) printf("%d\n", N);
    }
    printf("maxpid %d\n", maxpid);
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < adj[i].size(); j++) adj[i][j] = pid_to_cid[adj[i][j]];
        if (i % 10000 == 0) printf("%d\n", i);
    }
    unsigned seed = 0; //chrono::system_clock::now().time_since_epoch().count();
    shuffle(ordering, ordering+N, default_random_engine(seed));
    printf("loaded %d pages\n", N);

    for (int i = 0; i < N; i++) {
        int o = ordering[i];
        go(o);
        for (int j = 0; j < N; j++) {
            total_betweenness[j] += betweenness[j];
            total_betweenness_denom[j] += (double)(o != j ? N-2 : 0);
        }
        printf("[%d] %d\n", i, cid_to_pid[o]);
        for(int i = 0; i < N; i++) temp[i] = total_betweenness[i] / total_betweenness_denom[i];
        sort(temp, temp+N);
        for (int i = N-1; i >= N-100; i--) printf("%.3lf ", temp[i]);
        printf("\n");
    }

    for (int i = 0; i < N; i++) total_betweenness[i] /= total_betweenness_denom[i];
    sort(total_betweenness, total_betweenness+N);
    for (int i = N-1; i >= N-100; i--) printf("%.3lf ", total_betweenness[i]);
    printf("\n");
}
