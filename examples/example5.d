/++
 + Example 5 — directed weighted dependency graph (build-system simulation).
 +
 + Models a set of D packages as nodes in a directed acyclic graph (DAG).
 + Resolves the build order via topological sort (DFS), "compiles" each package
 + into a GC-managed artifact buffer, then lets the collector reclaim everything.
 +
 + Allocation strategy
 +   Node*          → allocate()       struct holds GC-managed name/edges/artifact ptrs
 +   name buffer    → allocateAtomic() pure byte data — GC must NOT scan it for ptrs
 +   Edge[] buffer  → allocateAtomic() {int, float} records — no embedded GC ptrs
 +   artifact data  → allocateAtomic() simulated compiled binary — no GC ptrs
 +   visited/stack  → allocateZeroed() DFS working buffers — zero-init required
 +
 + API surface exercised
 +   allocate, allocateAtomic, allocateZeroed
 +   resolveInternalPointer, isHeapPtr
 +   getHeapSize, freeBytes, totalBytes
 +   collect, ThreadGuard
 +/
import bdwgc;
import core.stdc.string : memset, strlen, memcpy;

extern (C) @nogc nothrow:

private:

enum MAX_EDGES   = 8;
enum ARTIFACT_SZ = 64 * 1024; // 64 KiB per compiled package

// ---------------------------------------------------------------------------
// Edge: {int, float} — purely numeric, no GC pointers.
// Stored in an allocateAtomic buffer; the GC will never scan it.
// ---------------------------------------------------------------------------
struct Edge
{
    int   to;     // destination node index
    float weight; // dependency priority (1.0 = normal, >1.0 = critical path)
}

// ---------------------------------------------------------------------------
// Node: contains GC-managed pointers (name, edges, artifact).
// Must be allocated with allocate() so the GC scans and traces those pointers.
// ---------------------------------------------------------------------------
struct Node
{
    char*  name;       // → allocateAtomic (pure byte data)
    Edge*  edges;      // → allocateAtomic ({int,float} records)
    ubyte* artifact;   // → allocateAtomic (compiled binary, filled after build)
    size_t artifactSz;
    int    edgeCount;
    int    edgeCap;
}

// ---------------------------------------------------------------------------
// Construction helpers
// ---------------------------------------------------------------------------

Node* makeNode(const(char)* name) @trusted
{
    // Node holds GC pointers → allocate so the GC traces name/edges/artifact.
    void[] nb = BoehmAllocator.instance.allocate(Node.sizeof);
    if (!nb.ptr)
        return null;
    auto n = cast(Node*) nb.ptr;

    // Name is raw bytes — no GC pointers inside → allocateAtomic.
    size_t len = strlen(name) + 1;
    void[] nameBuf = BoehmAllocator.instance.allocateAtomic(len);
    if (!nameBuf.ptr)
        return null;
    memcpy(nameBuf.ptr, name, len);
    n.name = cast(char*) nameBuf.ptr;

    // Edge array contains {int, float} — no GC pointers → allocateAtomic.
    void[] edgeBuf = BoehmAllocator.instance.allocateAtomic(Edge.sizeof * MAX_EDGES);
    if (!edgeBuf.ptr)
        return null;
    n.edges   = cast(Edge*) edgeBuf.ptr;
    n.edgeCap = MAX_EDGES;
    return n;
}

bool addEdge(Node* from, int to, float weight) @trusted @nogc
{
    if (!from || from.edgeCount >= from.edgeCap)
        return false;
    from.edges[from.edgeCount++] = Edge(to, weight);
    return true;
}

// Simulate compilation: allocate a 64 KiB artifact buffer filled with 0xCC
// (classic breakpoint-trap pattern used in debug object files).
// Binary data → allocateAtomic (GC skips scanning it for pointers).
bool compileNode(Node* n) @trusted
{
    void[] buf = BoehmAllocator.instance.allocateAtomic(ARTIFACT_SZ);
    if (!buf.ptr)
        return false;
    memset(buf.ptr, 0xCC, ARTIFACT_SZ);
    n.artifact   = cast(ubyte*) buf.ptr;
    n.artifactSz = ARTIFACT_SZ;
    return true;
}

// ---------------------------------------------------------------------------
// Topological sort — recursive DFS
// Working buffers use allocateZeroed so visited[] starts all-false
// and the index stack starts all-zero without manual initialisation.
// ---------------------------------------------------------------------------

void dfs(Node** nodes, int idx, bool* vis, int* stack, int* top) @trusted
{
    vis[idx] = true;
    Node* n = nodes[idx];
    foreach (i; 0 .. n.edgeCount)
        if (!vis[n.edges[i].to])
            dfs(nodes, n.edges[i].to, vis, stack, top);
    stack[(*top)++] = idx;
}

// Returns a GC-managed array of node indices ordered dependency-first.
int* topoSort(Node** nodes, int count) @trusted
{
    void[] visBuf = BoehmAllocator.instance.allocateZeroed(bool.sizeof * count);
    void[] stBuf  = BoehmAllocator.instance.allocateZeroed(int.sizeof  * count);
    if (!visBuf.ptr || !stBuf.ptr)
        return null;

    auto vis   = cast(bool*) visBuf.ptr;
    auto stack = cast(int*)  stBuf.ptr;
    int  top   = 0;

    foreach (i; 0 .. count)
        if (!vis[i])
            dfs(nodes, i, vis, stack, &top);

    return stack; // GC keeps this alive as long as the caller holds the pointer
}

// ---------------------------------------------------------------------------
// Heap-stats banner
// ---------------------------------------------------------------------------

void printStats(const(char)* phase) @trusted
{
    GC_printf("[%-18s]  heap %5zu KiB  free %5zu KiB  total %6zu KiB\n",
        phase,
        BoehmAllocator.instance.getHeapSize() / 1024,
        BoehmAllocator.instance.freeBytes()   / 1024,
        BoehmAllocator.instance.totalBytes()  / 1024);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

public:

void main() @trusted
{
    auto guard = ThreadGuard.create();
    printStats("init");

    // Dependency graph:
    //
    //   core <── algorithm ──┐
    //     ^                  +── format <── app
    //   container ───────────┘
    //
    enum N = 5;
    Node*[N] nodes;
    nodes[0] = makeNode("core");
    nodes[1] = makeNode("algorithm");
    nodes[2] = makeNode("container");
    nodes[3] = makeNode("format");
    nodes[4] = makeNode("app");
    foreach (n; nodes)
        assert(n !is null);

    // edges: "X depends on Y at priority W"
    addEdge(nodes[1], 0, 1.0f); // algorithm  → core
    addEdge(nodes[2], 0, 1.0f); // container  → core
    addEdge(nodes[3], 1, 0.8f); // format     → algorithm  (lower priority)
    addEdge(nodes[3], 2, 0.9f); // format     → container
    addEdge(nodes[4], 3, 1.0f); // app        → format

    printStats("graph built");

    // ── interior-pointer resolution ────────────────────────────────────────
    // "app" name was allocated with allocateAtomic; GC_base() still maps any
    // byte inside that block back to the block start.
    void* nameBase     = cast(void*) nodes[4].name;
    void* nameInterior = cast(void*)(nodes[4].name + 2); // offset into "app\0"
    void* resolved     = BoehmAllocator.instance.resolveInternalPointer(nameInterior);
    assert(resolved == nameBase);
    GC_printf("\nresolveInternalPointer: &\"app\"[2] -> block base  OK\n");

    // ── topological sort → dependency-first build order ───────────────────
    int* order = topoSort(nodes.ptr, N);
    assert(order !is null);

    GC_printf("\nBuild order (leaves first):\n");
    foreach (step; 0 .. N)
    {
        Node* n = nodes[order[step]];
        GC_printf("  %d. %-12s", step + 1, n.name);
        if (n.edgeCount > 0)
        {
            GC_printf("  deps:");
            foreach (e; 0 .. n.edgeCount)
                GC_printf("  %s(%.1f)", nodes[n.edges[e].to].name,
                    n.edges[e].weight);
        }
        else
        {
            GC_printf("  (no dependencies)");
        }
        GC_printf("\n");
    }

    // ── compile each package in dependency order ───────────────────────────
    GC_printf("\nCompiling:\n");
    foreach (step; 0 .. N)
    {
        Node* n = nodes[order[step]];
        assert(compileNode(n));

        // Verify interior pointer into the artifact block resolves correctly.
        void* mid  = cast(void*)(n.artifact + ARTIFACT_SZ / 2);
        void* base = BoehmAllocator.instance.resolveInternalPointer(mid);
        assert(base == cast(void*) n.artifact);
        assert(BoehmAllocator.instance.isHeapPtr(n.artifact));

        GC_printf("  built %-12s  artifact %zu KiB @ %p  OK\n",
            n.name, n.artifactSz / 1024, n.artifact);
    }

    printStats("after compile");

    // ── drop all references and reclaim ───────────────────────────────────
    // Nulling nodes[] removes the root references to every Node struct and
    // their associated name, edge, and artifact buffers. After collect() the
    // GC is free to reclaim all of that memory.
    GC_printf("\nReleasing graph, triggering collection...\n");
    foreach (ref n; nodes)
        n = null;
    order = null;

    BoehmAllocator.instance.collect();
    printStats("after collect");
}
