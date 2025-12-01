import bisect

class BPlusTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.children = [] 
        self.values = []  # Only leaf nodes hold values
        self.next = None  # Link to the next leaf node

class BPlusTree:
    def __init__(self, order=4):
        self.root = BPlusTreeNode(leaf=True)
        self.order = order

    def insert(self, key, value):
        # Start recursive insert from root
        result = self._insert_recursive(self.root, key, value)
        
        if result:
            # Root split
            split_key, new_node = result
            new_root = BPlusTreeNode(leaf=False)
            new_root.keys = [split_key]
            new_root.children = [self.root, new_node]
            self.root = new_root

    def _insert_recursive(self, node, key, value):
        if node.leaf:
            # Insert into leaf
            bisect.insort(node.keys, key)
            idx = node.keys.index(key)
            node.values.insert(idx, value)
            
            # Check for overflow
            if len(node.keys) > (self.order - 1):
                return self._split_node(node)
            return None
        else:
            # Internal node
            idx = bisect.bisect_right(node.keys, key)
            child = node.children[idx]
            
            result = self._insert_recursive(child, key, value)
            
            if result:
                split_key, new_child = result
                # Insert promoted key and new child into current node
                node.keys.insert(idx, split_key)
                node.children.insert(idx + 1, new_child)
                
                # Check for overflow
                if len(node.keys) > (self.order - 1):
                    return self._split_node(node)
            
            return None

    def _split_node(self, node):
        mid = self.order // 2
        new_node = BPlusTreeNode(leaf=node.leaf)
        
        if node.leaf:
            # Leaf Split: Middle key is COPIED up
            # Split point: mid
            # Left: [:mid], Right: [mid:]
            # Promoted key: keys[mid]
            
            split_key = node.keys[mid]
            
            new_node.keys = node.keys[mid:]
            new_node.values = node.values[mid:]
            
            node.keys = node.keys[:mid]
            node.values = node.values[:mid]
            
            # Link leaves
            new_node.next = node.next
            node.next = new_node
            
            return split_key, new_node
            
        else:
            # Internal Split: Middle key is PUSHED up
            # Split point: mid
            # Left: [:mid], Right: [mid+1:]
            # Promoted key: keys[mid]
            
            split_key = node.keys[mid]
            
            new_node.keys = node.keys[mid+1:]
            new_node.children = node.children[mid+1:]
            
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
            
            return split_key, new_node

    def search(self, key):
        # Returns metadata and a trace of the path taken
        current = self.root
        path = ["Root"]
        
        while not current.leaf:
            path.append(f"Internal Node (keys: {current.keys})")
            idx = bisect.bisect_right(current.keys, key)
            current = current.children[idx]
            
        path.append(f"Leaf Node (keys: {current.keys})")
        
        if key in current.keys:
            idx = current.keys.index(key)
            return current.values[idx], path
        return None, path

    def range_search(self, start_key, end_key):
        # 1. Find the starting leaf node
        current = self.root
        path = ["Root"]
        while not current.leaf:
            idx = bisect.bisect_right(current.keys, start_key)
            current = current.children[idx]
            path.append(f"Internal Node (keys: {current.keys})")
            
        # 2. Traverse linked leaves
        results = []
        path.append(f"Start Leaf (keys: {current.keys})")
        
        while current:
            for i, key in enumerate(current.keys):
                if key >= start_key:
                    if key <= end_key:
                        results.append({
                            "key": key,
                            "value": current.values[i]
                        })
                    else:
                        # Exceeded end_key
                        return results, path
            current = current.next
            if current:
                 path.append(f"Next Leaf (keys: {current.keys})")
                 
        return results, path

    def get_tree_structure(self):
        # Recursive helper to return nested structure
        def serialize(node):
            return {
                "keys": node.keys,
                "values": node.values if node.leaf else [],
                "type": "Leaf" if node.leaf else "Internal",
                "children": [serialize(child) for child in node.children] if not node.leaf else []
            }
        return serialize(self.root)
