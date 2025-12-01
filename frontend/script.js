const API_BASE = 'http://localhost:5000';

const keyInput = document.getElementById('key-input');
const valueInput = document.getElementById('value-input');
const insertBtn = document.getElementById('insert-btn');
const searchBtn = document.getElementById('search-btn');
const optimizationToggle = document.getElementById('optimization-toggle');
const treeContainer = document.getElementById('tree-container');
const searchResult = document.getElementById('search-result');
const timeTakenDisplay = document.getElementById('time-taken');
const methodDisplay = document.getElementById('method-used');
const ioCostDisplay = document.getElementById('io-cost');
const networkLogs = document.getElementById('network-logs');

// Helper to log network activity
function logNetwork(message) {
    const li = document.createElement('li');
    li.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    networkLogs.prepend(li);
}

// Helper to flash storage node
function flashNode(nodeId) {
    const node = document.getElementById(nodeId);
    if (node) {
        node.classList.add('active');
        setTimeout(() => {
            node.classList.remove('active');
        }, 500);
    }
}

// Render Tree Visualization
function renderTree(structure) {
    treeContainer.innerHTML = '';

    if (!structure || !structure.keys) {
        treeContainer.innerHTML = '<div class="placeholder-text">Tree is empty.</div>';
        return;
    }

    const treeRoot = document.createElement('div');
    treeRoot.className = 'tf-tree';

    const ul = document.createElement('ul');
    ul.appendChild(createTreeNode(structure));
    treeRoot.appendChild(ul);

    treeContainer.appendChild(treeRoot);
}

function createTreeNode(nodeData) {
    const li = document.createElement('li');

    const nodeContent = document.createElement('div');
    nodeContent.className = `tf-nc ${nodeData.type.toLowerCase()}`;

    let html = `<div class="keys">[${nodeData.keys.join(', ')}]</div>`;

    if (nodeData.type === 'Leaf' && nodeData.values && nodeData.values.length > 0) {
        const vals = nodeData.values.map(v => {
            if (v && v.node_id) {
                return v.node_id.replace('node_', 'N');
            }
            return '?';
        }).join(', ');
        html += `<div class="values">Loc: [${vals}]</div>`;
    }

    nodeContent.innerHTML = html;
    li.appendChild(nodeContent);

    if (nodeData.children && nodeData.children.length > 0) {
        const ul = document.createElement('ul');
        nodeData.children.forEach(child => {
            ul.appendChild(createTreeNode(child));
        });
        li.appendChild(ul);
    }

    return li;
}

// Insert Data
insertBtn.addEventListener('click', async () => {
    const key = parseInt(keyInput.value);
    const value = valueInput.value;

    if (!key || !value) {
        alert('Please enter both Key and Value');
        return;
    }

    insertBtn.disabled = true;
    logNetwork(`Inserting Key: ${key}...`);

    try {
        const response = await fetch(`${API_BASE}/insert`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key, value })
        });

        const data = await response.json();

        if (data.status === 'success') {
            logNetwork(`Stored in ${data.location.node_id}`);
            flashNode(data.location.node_id);
            renderTree(data.tree_structure);
            searchResult.textContent = `Inserted successfully into ${data.location.node_id}`;
        }
    } catch (error) {
        console.error('Error:', error);
        searchResult.textContent = 'Error inserting data.';
    } finally {
        insertBtn.disabled = false;
    }
});

// Search Data
searchBtn.addEventListener('click', async () => {
    const key = keyInput.value;
    const optimized = optimizationToggle.checked;

    if (!key) {
        alert('Please enter a Key to search');
        return;
    }

    searchBtn.disabled = true;
    searchResult.textContent = 'Searching...';
    timeTakenDisplay.textContent = '...';
    methodDisplay.textContent = optimized ? 'B+ Tree Optimized' : 'Linear Scan';
    ioCostDisplay.textContent = '...';

    logNetwork(`Searching Key: ${key} (Optimized: ${optimized})`);

    try {
        const response = await fetch(`${API_BASE}/search?key=${key}&optimized=${optimized}`);
        const data = await response.json();

        if (data.result) {
            searchResult.textContent = JSON.stringify(data.result, null, 2);
            timeTakenDisplay.textContent = data.io_cost.toFixed(2);
            ioCostDisplay.textContent = data.path_taken;

            if (optimized) {
                // Highlight path logic could go here if we had node IDs in the tree structure
                // For now, just flash the final storage node
                flashNode(data.result.node_id);
            } else {
                // Unoptimized: Flash visited nodes sequentially
                if (data.visited_nodes) {
                    data.visited_nodes.forEach((id, idx) => {
                        setTimeout(() => flashNode(id), idx * 500);
                    });
                }
            }
        } else {
            searchResult.textContent = 'Not Found';
            timeTakenDisplay.textContent = data.io_cost.toFixed(2);
            ioCostDisplay.textContent = data.path_taken;

            // Even if not found, flash the nodes we visited (which would be all of them in a full scan)
            if (!optimized && data.visited_nodes) {
                data.visited_nodes.forEach((id, idx) => {
                    setTimeout(() => flashNode(id), idx * 500);
                });
            }
        }
    } catch (error) {
        console.error('Error:', error);
        searchResult.textContent = 'Error searching data.';
    } finally {
        searchBtn.disabled = false;
    }
});

// Initial Tree Load
async function loadTree() {
    try {
        const response = await fetch(`${API_BASE}/tree`);
        const structure = await response.json();
        renderTree(structure);
    } catch (e) {
        console.log("Backend not ready yet");
    }
}

loadTree();
