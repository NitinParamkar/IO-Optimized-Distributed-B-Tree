import time
import uuid
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

class StorageManager:
    def __init__(self):
        # Connect to 3 separate MongoDB instances (Nodes)
        self.clients = {
            "node_1": MongoClient(os.getenv("DATABASE_URL1")),
            "node_2": MongoClient(os.getenv("DATABASE_URL2")),
            "node_3": MongoClient(os.getenv("DATABASE_URL3"))
        }
        
        self.dbs = {
            "node_1": self.clients["node_1"].get_database("distritree_db"),
            "node_2": self.clients["node_2"].get_database("distritree_db"),
            "node_3": self.clients["node_3"].get_database("distritree_db")
        }
        
        self.collections = {
            "node_1": self.dbs["node_1"]["records"],
            "node_2": self.dbs["node_2"]["records"],
            "node_3": self.dbs["node_3"]["records"]
        }

    def store_data(self, key, value):
        # Use the Key (Student ID) to decide which node to store on.
        # This ensures deterministic distribution.
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
            "record_id": record_id,
            "key": key,
            "value": value,
            "timestamp": time.time()
        }
        
        # Actual MongoDB Insert (No artificial sleep)
        self.collections[target_node].insert_one(record)
        
        return {"node_id": target_node, "record_id": record_id}

    def fetch_data(self, node_id, record_id):
        # Actual MongoDB Find (No artificial sleep)
        record = self.collections[node_id].find_one({"record_id": record_id})
        if record:
            record.pop('_id', None) # Remove MongoDB internal ID
        return record

    def scan_all(self, target_key):
        # Simulates a linear scan across all nodes (High Latency)
        start_time = time.time()
        found_data = None
        path_taken = []
        
        # Convert target_key to the same type as stored (int if possible)
        try:
            target_key = int(target_key)
        except:
            pass

        visited_nodes = []
        for node_id, collection in self.collections.items():
            path_taken.append(f"Scanning {node_id}...")
            visited_nodes.append(node_id)
            
            # Actual MongoDB Query
            # In a true linear scan without an index on 'key', we might scan everything.
            # But here we just query the collection. To strictly simulate "scan", 
            # we could iterate, but finding by key is what we want functionally.
            # However, since this is "unoptimized", we iterate all 3 nodes.
            
            record = collection.find_one({"key": target_key})
            
            if record:
                record.pop('_id', None)
                found_data = {"node_id": node_id, "record_id": record['record_id'], "data": record}
                path_taken.append(f"FOUND in {node_id}")
                break
            
        end_time = time.time()
        return {
            "result": found_data,
            "io_cost": (end_time - start_time) * 1000, # ms
            "path_taken": " -> ".join(path_taken),
            "visited_nodes": visited_nodes
        }

    def scan_range(self, start_key, end_key):
        # Simulates a linear scan across all nodes to find keys in range
        start_time = time.time()
        results = []
        path_taken = []
        visited_nodes = []
        
        try:
            start_key = int(start_key)
            end_key = int(end_key)
        except:
            pass
            
        for node_id, collection in self.collections.items():
            path_taken.append(f"Scanning {node_id}...")
            visited_nodes.append(node_id)
            
            # MongoDB Range Query
            cursor = collection.find({"key": {"$gte": start_key, "$lte": end_key}})
            
            for record in cursor:
                record.pop('_id', None)
                results.append({
                    "key": record['key'],
                    "value": record
                })
                    
        end_time = time.time()
        return {
            "results": sorted(results, key=lambda x: x['key']),
            "io_cost": (end_time - start_time) * 1000,
            "path_taken": " -> ".join(path_taken),
            "visited_nodes": visited_nodes
        }

    def clear_all_data(self):
        for node_id, collection in self.collections.items():
            collection.delete_many({})

    def close(self):
        for client in self.clients.values():
            client.close()
