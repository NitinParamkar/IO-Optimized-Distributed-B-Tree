from flask import Flask, jsonify, request
from flask_cors import CORS
from storage_manager import StorageManager
from bplustree import BPlusTree
import time

app = Flask(__name__)
CORS(app) # Enable CORS for frontend

storage = StorageManager()
bptree = BPlusTree(order=4) 

@app.route('/insert', methods=['POST'])
def insert():
    data = request.json
    key = data.get('key')
    value = data.get('value')
    
    if not key or not value:
        return jsonify({"error": "Key and Value are required"}), 400

    # 1. Store data in the "Slow" Storage Network
    # We pass the Key now so we can distribute based on it
    location_metadata = storage.store_data(key, value)
    
    # 2. Store the POINTER in the "Fast" B+ Tree
    bptree.insert(key, location_metadata)
    
    return jsonify({
        "status": "success", 
        "location": location_metadata,
        "tree_structure": bptree.get_tree_structure()
    })

@app.route('/search', methods=['GET'])
def search():
    key = request.args.get('key')
    optimized = request.args.get('optimized', 'true').lower() == 'true'
    
    if not key:
        return jsonify({"error": "Key is required"}), 400

    # Ensure key type matches insertion (Integer)
    try:
        key = int(key)
    except ValueError:
        pass # Keep as string if conversion fails


    start_time = time.time()
    
    if optimized:
        # 1. Search the Tree (Fast, In-Memory)
        metadata, path = bptree.search(key)
        
        if metadata:
            # 2. Fetch from Storage (Slow, Simulated IO)
            data = storage.fetch_data(metadata['node_id'], metadata['record_id'])
            path.append(f"Storage Node: {metadata['node_id']}")
            result = {"data": data, "node_id": metadata['node_id']}
        else:
            data = None
            result = None
            path.append("Not Found")
            
        end_time = time.time()
        
        return jsonify({
            "result": result,
            "io_cost": (end_time - start_time) * 1000, # ms
            "path_taken": " -> ".join(path),
            "method": "B+ Tree Optimized"
        })
    else:
        # Unoptimized: Linear Scan
        # We search the storage directly for the Key
        scan_result = storage.scan_all(key)
        
        return jsonify({
            "result": scan_result['result'],
            "io_cost": scan_result['io_cost'],
            "path_taken": scan_result['path_taken'],
            "visited_nodes": scan_result['visited_nodes'],
            "method": "Linear Scan (Unoptimized)"
        })

@app.route('/tree', methods=['GET'])
def get_tree():
    return jsonify(bptree.get_tree_structure())

if __name__ == '__main__':
    app.run(debug=True, port=5000)
