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
        root = self.root
        if len(root.keys) == (self.order - 1):
            new_root = BPlusTreeNode(leaf=False)
            self.root = new_root
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self._insert_non_full(new_root, key, value)
        else:
            self._insert_non_full(root, key, value)

    def _insert_non_full(self, node, key, value):
        if node.leaf:
            # Insert into leaf node
            bisect.insort(node.keys, key)
            idx = node.keys.index(key)
            node.values.insert(idx, value)
        else:
            # Internal node
            idx = bisect.bisect_right(node.keys, key)
            child = node.children[idx]
            if len(child.keys) == (self.order - 1):
                self._split_child(node, idx)
                if key >= node.keys[idx]:
                    idx += 1
            self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent, idx):
        child = parent.children[idx]
        new_child = BPlusTreeNode(leaf=child.leaf)
        
        mid = (self.order) // 2
        
        if child.leaf:
            # Leaf Split: Middle key is COPIED up
            split_key = child.keys[mid]
            
            # Right split
            new_child.keys = child.keys[mid:]
            new_child.values = child.values[mid:]
            
            # Left split
            child.keys = child.keys[:mid]
            child.values = child.values[:mid]
            
            # Link leaves
            new_child.next = child.next
            child.next = new_child
            
            parent.keys.insert(idx, split_key)
            parent.children.insert(idx + 1, new_child)
            
        else:
            # Internal Split: Middle key is PUSHED up
            mid = (self.order - 1) // 2
            split_key = child.keys[mid]
            
            new_child.keys = child.keys[mid+1:]
            new_child.children = child.children[mid+1:]
            
            child.keys = child.keys[:mid]
            child.children = child.children[:mid+1]
            
            parent.keys.insert(idx, split_key)
            parent.children.insert(idx + 1, new_child)

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
