import bisect

class BPlusTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.values = []  # In B-Tree, all nodes hold values
        self.children = [] 
        # self.next = None # No linked leaves in B-Tree

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
        if key in node.keys:
            # Update existing key (B-Tree property: Key exists only once)
            idx = node.keys.index(key)
            node.values[idx] = value
            return

        if node.leaf:
            bisect.insort(node.keys, key)
            idx = node.keys.index(key)
            node.values.insert(idx, value)
        else:
            idx = bisect.bisect_right(node.keys, key)
            child = node.children[idx]
            if len(child.keys) == (self.order - 1):
                self._split_child(node, idx)
                if key > node.keys[idx]:
                    idx += 1
                elif key == node.keys[idx]:
                    # Key was promoted to this node
                    node.values[idx] = value
                    return
            self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent, idx):
        child = parent.children[idx]
        new_child = BPlusTreeNode(leaf=child.leaf)
        mid = (self.order - 1) // 2
        
        # B-Tree Split: Middle Key AND Value move UP to parent
        # They are REMOVED from the child
        mid_key = child.keys[mid]
        mid_value = child.values[mid]
        
        parent.keys.insert(idx, mid_key)
        parent.values.insert(idx, mid_value)
        parent.children.insert(idx + 1, new_child)
        
        # Right side moves to new_child
        new_child.keys = child.keys[mid+1:]
        new_child.values = child.values[mid+1:]
        
        # Left side stays in child (excluding mid)
        child.keys = child.keys[:mid]
        child.values = child.values[:mid]
        
        if not child.leaf:
            new_child.children = child.children[mid+1:]
            child.children = child.children[:mid+1]

    def search(self, key):
        # Returns metadata and a trace of the path taken
        current = self.root
        path = ["Root"]
        
        while True:
            if key in current.keys:
                idx = current.keys.index(key)
                path.append(f"Found in Node (keys: {current.keys})")
                return current.values[idx], path
            
            if current.leaf:
                path.append(f"Leaf Node (keys: {current.keys})")
                return None, path
                
            idx = bisect.bisect_right(current.keys, key)
            path.append(f"Internal Node (keys: {current.keys})")
            current = current.children[idx]

    def get_tree_structure(self):
        # Helper to visualize the tree
        levels = []
        queue = [(self.root, 0)]
        
        while queue:
            node, level = queue.pop(0)
            if len(levels) <= level:
                levels.append([])
            
            node_data = {
                "keys": node.keys,
                "type": "Leaf" if node.leaf else "Internal"
            }
            levels[level].append(node_data)
            
            if not node.leaf:
                for child in node.children:
                    queue.append((child, level + 1))
        return levels
