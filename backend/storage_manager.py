import time
import uuid
import random

class StorageManager:
    def __init__(self):
        # Simulating 3 separate storage servers
        self.nodes = {
            "node_1": {},
            "node_2": {},
            "node_3": {}
        }

    def store_data(self, key, value):
        # Use the Key (Student ID) to decide which node to store on.
        # This ensures deterministic distribution.
        # Example: Key 1 -> Node 2, Key 2 -> Node 3, Key 3 -> Node 1
        try:
            key_int = int(key)
            node_index = (key_int % 3) + 1
        except ValueError:
            # Fallback for non-integer keys
            node_index = (hash(str(key)) % 3) + 1
            
        target_node = f"node_{node_index}"
        record_id = str(uuid.uuid4())
        
        # Store the full record including the key so we can verify it later
        record = {
            "key": key,
            "value": value,
            "timestamp": time.time()
        }
        
        # Simulating IO Write Delay
        time.sleep(0.1) 
        self.nodes[target_node][record_id] = record
        
        return {"node_id": target_node, "record_id": record_id}

    def fetch_data(self, node_id, record_id):
        # Simulating Network/Disk Read Delay
        time.sleep(0.5) 
        return self.nodes.get(node_id, {}).get(record_id, None)

    def scan_all(self, target_key):
        # Simulates a linear scan across all nodes (High Latency)
        # We have to check every single record in every node because we don't know where it is.
        start_time = time.time()
        found_data = None
        path_taken = []
        
        # Convert target_key to the same type as stored (int if possible)
        try:
            target_key = int(target_key)
        except:
            pass

        visited_nodes = []
        for node_id, records in self.nodes.items():
            path_taken.append(f"Scanning {node_id}...")
            visited_nodes.append(node_id)
            # Simulate network latency for accessing each node
            time.sleep(0.5) 
            
            for rid, record in records.items():
                # Check if this record matches the key we are looking for
                if record.get("key") == target_key:
                    found_data = {"node_id": node_id, "record_id": rid, "data": record}
                    path_taken.append(f"FOUND in {node_id}")
                    break
            
            if found_data:
                break
                
        end_time = time.time()
        return {
            "result": found_data,
            "io_cost": (end_time - start_time) * 1000, # ms
            "path_taken": " -> ".join(path_taken),
            "visited_nodes": visited_nodes
        }
