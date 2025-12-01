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
        start_time = time.time()
        # 1. Get pointers from B+ Tree (Fast)
        index_results, path = bptree.range_search(start_key, end_key)
        
        final_results = []
        visited_nodes = set()
        
        # 2. Fetch actual data from Storage (Slow Random IO)
        for r in index_results:
            val = r.get('value')
            if isinstance(val, dict) and 'node_id' in val:
                node_id = val['node_id']
                record_id = val['record_id']
                visited_nodes.add(node_id)
                
                # Simulate fetching the actual record
                record = storage.fetch_data(node_id, record_id)
                if record:
                    final_results.append({
                        "key": r['key'],
                        "value": record # Return the full record
                    })
        
        end_time = time.time()
        
        return jsonify({
            "results": final_results,
            "path_taken": " -> ".join(path),
            "io_cost": (end_time - start_time) * 1000, # ms
            "visited_nodes": list(visited_nodes)
        })
    else:
        # Unoptimized: Linear Scan
        scan_result = storage.scan_range(start_key, end_key)
        
        return jsonify({
            "results": scan_result['results'],
            "io_cost": scan_result['io_cost'],
            "path_taken": scan_result['path_taken'],
            "visited_nodes": scan_result['visited_nodes']
        })

@app.route('/tree', methods=['GET'])
def get_tree():
    return jsonify(bptree.get_tree_structure())

if __name__ == '__main__':
    app.run(debug=True, port=5000)
