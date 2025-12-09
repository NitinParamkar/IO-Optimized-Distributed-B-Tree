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

    try:
        key = int(key)
    except ValueError:
        return jsonify({"error": "Key must be an integer"}), 400

    # 0. Check for Duplicates (Unique Constraint)
    existing_metadata, _ = bptree.search(key)
    if existing_metadata:
         return jsonify({"error": f"Key {key} already exists! Duplicates not allowed."}), 409
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


    start_time = time.perf_counter()
    
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
            
        end_time = time.perf_counter()
        
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

@app.route('/range', methods=['GET'])
def range_search():
    start_key = request.args.get('start')
    end_key = request.args.get('end')
    optimized = request.args.get('optimized', 'true').lower() == 'true'
    
    if not start_key or not end_key:
        return jsonify({"error": "Start and End keys are required"}), 400
        
    try:
        start_key = int(start_key)
        end_key = int(end_key)
    except ValueError:
        return jsonify({"error": "Keys must be integers"}), 400
        
    if optimized:
        start_time = time.perf_counter()
        # 1. Get pointers from B+ Tree (Fast)
        index_results, path = bptree.range_search(start_key, end_key)
        
        # 2. Fetch actual data from Storage (Simulating Random I/O)
        # BATCH OPTIMIZATION: We group keys by Server (Node) to minimize network calls.
        
        # Step A: Group by Node
        grouped_requests = {}
        for r in index_results:
            metadata = r['value']
            node_id = metadata['node_id']
            record_id = metadata['record_id']
            
            if node_id not in grouped_requests:
                grouped_requests[node_id] = []
            grouped_requests[node_id].append(record_id)
            
        final_results = []
        
        # Step B: Execute Batch Requests (One per Node)
        for node_id, record_ids in grouped_requests.items():
            # ONE network call per server involved
            batch_records = storage.fetch_batch(node_id, record_ids)
            for rec in batch_records:
                final_results.append({
                    "key": rec['key'],
                    "value": rec['value']
                })

        # Sort the final results because batch fetching might mess up the order
        final_results.sort(key=lambda x: x['key'])
        
        # Collect visited nodes for visualization (from the metadata)
        visited_nodes = set()
        for r in index_results:
            val = r.get('value')
            if isinstance(val, dict) and 'node_id' in val:
                visited_nodes.add(val['node_id'])
        
        end_time = time.perf_counter()
        
        return jsonify({
            "results": final_results,
            "path_taken": " -> ".join(path),
            "io_cost": (end_time - start_time) * 1000, # ms
            "visited_nodes": list(visited_nodes)
        })
    else:
        # Unoptimized: Linear Scan
        scan_result = storage.scan_range(start_key, end_key)
        
        # Extract keys and values
        # storage.scan_range returns items as {'key': k, 'value': record_dict}
        formatted_results = []
        for r in scan_result['results']:
            # r['value'] is the full mongo record, so we grab r['value']['value']
            val = r['value'].get('value', '?')
            formatted_results.append({
                "key": r['key'],
                "value": val
            })
        
        return jsonify({
            "results": formatted_results,
            "io_cost": scan_result['io_cost'],
            "path_taken": scan_result['path_taken'],
            "visited_nodes": scan_result['visited_nodes']
        })

@app.route('/clear', methods=['POST'])
def clear_data():
    # 1. Clear MongoDB Data
    storage.clear_all_data()
    
    # 2. Reset B+ Tree (In-Memory)
    global bptree
    bptree = BPlusTree(order=4)
    
    return jsonify({"status": "success", "message": "All data cleared from Storage Nodes and B+ Tree"})

@app.route('/tree', methods=['GET'])
def get_tree():
    return jsonify(bptree.get_tree_structure())



import atexit

atexit.register(storage.close)

if __name__ == '__main__':
    print("Clearing previous data from MongoDB...")
    storage.clear_all_data()
    app.run(debug=True, port=5000, use_reloader=False)
