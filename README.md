# DistriTree: IO-Optimized Distributed B+ Tree Indexing System

## 1. Problem Statement
In large-scale distributed storage systems (like SANs or cloud storage), data is scattered across multiple physical nodes (servers/disks). 

**The Challenge:**
- **Network Latency:** Accessing a remote server is significantly slower than accessing local memory (RAM).
- **Linear Scanning:** Without an index, finding a specific record requires querying every single node and scanning every record. This results in $O(N)$ network calls, which is prohibitively slow.
- **Bottleneck:** The network I/O becomes the primary bottleneck, making retrieval times unacceptable for real-time applications.

## 2. Our Solution
**DistriTree** solves this by implementing a centralized **B+ Tree Index** that resides in the "Fast" memory (RAM) of an Index Server.

**The Approach:**
1.  **In-Memory Index:** We maintain a B+ Tree structure where:
    -   **Keys** (e.g., Student IDs) are stored in internal nodes for navigation.
    -   **Values** (Pointers to Storage Nodes) are stored in the tree nodes.
2.  **Deterministic Routing:** The index tells us *exactly* which Storage Node holds the data.
3.  **O(1) Network Call:** Instead of broadcasting "Who has ID 10?" to all servers, we look up ID 10 in the B+ Tree, get "Server 2", and make **one single network request** to Server 2.

**Key Result:** We reduce the search complexity from $O(N)$ network calls (Linear Scan) to $O(log_M N)$ RAM lookups + **1 Network Call**.

## 3. Project Description
This project is a simulation of the above architecture, designed to visually demonstrate the performance gap.

### Architecture
-   **Frontend (Dashboard):** A visual control panel to insert data, search for keys, and view the system state.
    -   **Visual Tree:** Displays the live B+ Tree structure growing and splitting.
    -   **Storage Nodes:** Simulates 3 remote servers (Node 1, Node 2, Node 3).
    -   **Animations:** visually shows the "path" taken by the search algorithm.
-   **Backend (Python/Flask):**
    -   **B+ Tree Logic:** A custom implementation of a B+ Tree (Order 4) that handles splitting and indexing.
    -   **Storage Simulation:** A `StorageManager` that introduces artificial latency (500ms) to simulate real-world network/disk I/O.
    -   **API:** Endpoints to `insert` and `search` data.

### Features
1.  **Distributed Storage Simulation:** Data is sharded across 3 nodes based on the Key (`Key % 3`).
2.  **Real-Time Visualization:** Watch the Tree grow and nodes flash as they are accessed.
3.  **Optimization Toggle:**
    -   **Optimized (B+ Tree):** Instant lookup. Highlights the path in the tree and accesses only the correct storage node.
    -   **Unoptimized (Linear Scan):** Slow. Visually scans every server one by one until data is found.
4.  **Metrics:** Displays the "Time Taken" and "IO Cost" to prove the efficiency of the B+ Tree.

### How to Run
1.  **Backend:**
    ```bash
    cd backend
    python -m venv .venv
    .venv\Scripts\activate
    pip install -r requirements.txt
    python app.py
    
    ```
2.  **Frontend:**
    -   Open `frontend/index.html` in any modern web browser.

### Tech Stack
-   **Frontend:** HTML5, CSS3 (Dark Theme), Vanilla JavaScript.
-   **Backend:** Python 3, Flask.
