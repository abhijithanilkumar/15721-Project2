#pragma once

#include <functional>
#include "common/spin_latch.h"
#include "common/macros.h"
#include <list>
#include <vector>
#include <iterator>
#include <stack>



namespace terrier::storage::index {

#define FAN_OUT 10
// Ceil (FAN_OUT / 2) - 1
#define MIN_KEYS_INNER_NODE 4
// Ceil ((FAN_OUT - 1) / 2)
#define MIN_KEYS_LEAF_NODE 5

template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
    typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
    typename ValueEqualityChecker = std::equal_to<ValueType>>
class BPlusTree {
  static inline KeyComparator key_cmp_obj{};
  static inline KeyEqualityChecker key_eq_chk{};
  static inline ValueEqualityChecker val_eq_chk{};

  mutable common::SpinLatch tree_latch_;
  /*
   * class ElasticNode - The base class for elastic node types, i.e. InnerNode
   *                     and LeafNode
   *
   * Since for InnerNode and LeafNode, the number of elements is not a compile
   * time known constant. However, for efficient tree traversal we must inline
   * all elements to reduce cache misses with workload that's less predictable
   */
  class Node {
   private:

   public:

    /*
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */

    Node() {}
    ~Node() {}

    virtual Node* Split() = 0;
    virtual void SetPrevPtr(Node* ptr) = 0;
    virtual bool isLeaf() = 0;
    virtual size_t GetHeapSpaceSubtree() = 0;
  };

  // Root of the tree
  Node* root;
  using KeyValueListPair = std::pair<KeyType, std::list<ValueType>>;
  using KeyNodePtrPair = std::pair<KeyType, Node*>;

  class LeafNode: public Node {

   private:
    friend class BPlusTree;

    std::vector<std::pair<KeyType, std::list<ValueType>>> entries;
    // Sibling pointers
    LeafNode *prev_ptr;
    LeafNode *next_ptr;

    // TODO: Optimize and use binary search
    uint64_t GetPositionToInsert(const KeyType& key) {
      int i;

      for (i = 0; i < entries.size(); i++) {
        if (!key_cmp_obj(entries[i].first, key)) {
          break;
        }
      }

      return i;
    }

    typename std::vector<KeyValueListPair>::iterator GetPositionOfKey(const KeyType& key) {
      auto it = entries.begin();

      while(it != entries.end()) {
        if (key_eq_chk(it->first, key)) return it;
        it++;
      }

      return it;
    }

   public:
    LeafNode() {
      prev_ptr = nullptr;
      next_ptr = nullptr;
    }

    ~LeafNode() {}

    bool IsOverflow() {
      uint64_t size = entries.size();
      return (size >= FAN_OUT);
    }

    void SetPrevPtr(Node* ptr) {
      prev_ptr = dynamic_cast<LeafNode*>(ptr);
    }

    bool HasKey(const KeyType &key) {
      // TODO: Optimize using STL function
      for(auto it = entries.begin(); it != entries.end(); it++) {
        if (key_eq_chk(it->first, key)) return  true;
      }

      return false;
    }

    bool HasKeyValue(const KeyType &key, const ValueType &value) {
      // TODO: Optimize using STL function
      for (auto it = entries.begin(); it != entries.end(); it++) {
        if (key_eq_chk(it->first, key)) {
          for (auto val_iter = it->second.begin(); val_iter != it->second.end(); val_iter++) {
            if (val_eq_chk(*val_iter, value)) return true;
          }
        }
      }

      return false;
    }

    void Insert(const KeyType& key, const ValueType& value) {
      uint64_t pos_to_insert = GetPositionToInsert(key);

      if (pos_to_insert < entries.size() && key_eq_chk(entries[pos_to_insert].first, key)) {
        entries[pos_to_insert].second.push_back(value);
      } else {
        std::pair<KeyType, std::list<ValueType>> new_pair;
        new_pair.first = key;
        new_pair.second.push_back(value);
        entries.insert(entries.begin() + pos_to_insert, new_pair);
      }
    }

    KeyType GetFirstKey() {
      return entries[0].first;
    }

    Node* Split() {
      LeafNode* new_node = new LeafNode();

      // Copy the right half entries to the next node
      new_node->Copy(entries.begin() + MIN_KEYS_LEAF_NODE, entries.end());

      // Erase the right half from the current node
      entries.erase(entries.begin() + MIN_KEYS_LEAF_NODE, entries.end());

      // Set the forward sibling pointer of the current node
      next_ptr = new_node;

      // Set the backward sibling pointer of the new node
      new_node->SetPrevPtr(this);

      return new_node;
    }

    void Copy(typename std::vector<KeyValueListPair>::iterator begin,
        typename std::vector<KeyValueListPair>::iterator end) {
      entries.insert(entries.end(), begin, end);
    }

    bool SatisfiesPredicate(const KeyType& key, std::function<bool(const ValueType)> predicate) {
      auto it = GetPositionOfKey(key);

      if (it == entries.end()) return false;

      for (auto i = (it->second).begin(); i != (it->second).end(); i++) {
        if (predicate(*i)) return true;
      }

      return false;
    }

    bool isLeaf() {
      return true;
    }

    void ScanAndPopulateResults(const KeyType &key, typename std::vector<ValueType> &results) {
      auto it = GetPositionOfKey(key);

      if (it == entries.end()) return;

      for (auto i = (it->second).begin(); i != (it->second).end(); i++) {
        results.push_back(*i);
      }
    }

    size_t GetHeapSpaceSubtree() {
      size_t size = 0;
      // Current node's heap space used
      for (auto it = entries.begin(); it != entries.end(); ++it) {
        size += (it->second).size() * sizeof(ValueType) + sizeof(KeyType);
      }
      return size;
    }
  };

  class InnerNode: public Node {
   private:
    std::vector<KeyNodePtrPair> entries;
    friend class BPlusTree;

    // n pointers, n-1 keys
    Node* prev_ptr;

   public:

    InnerNode() {
      prev_ptr = nullptr;
    }

    ~InnerNode() {}

    // TODO: Optimize and use binary search
    uint64_t GetPositionGreaterThanEqualTo(const KeyType& key) {
      int i;

      for (i = 0; i < entries.size(); i++) {
        if (!key_cmp_obj(entries[i].first, key)) {
          break;
        }
      }

      return i;
    }

    bool IsOverflow() {
      uint64_t size = entries.size();
      return (size >= FAN_OUT);
    }

    void Insert(const KeyType& key, Node* node_ptr) {
      uint64_t pos_to_insert = GetPositionGreaterThanEqualTo(key);

      // We are sure that there is no duplicate key
      KeyNodePtrPair new_pair;
      new_pair.first = key;
      new_pair.second = node_ptr;
      entries.insert(entries.begin() + pos_to_insert, new_pair);
    }

    void SetPrevPtr(Node* node) {
      prev_ptr = node;
    }

    void InsertNodePtr(Node* child_node, Node** tree_root, std::stack<InnerNode*> &node_traceback) {
      InnerNode* current_node = this;
      // Since child_node is always a leaf node, we do not remove first key here
      TERRIER_ASSERT(child_node->isLeaf(), "child_node has to be a leaf node");
      KeyType middle_key = dynamic_cast<LeafNode*>(child_node)->GetFirstKey();

      // If current node is the root or has a parent
      while (current_node == *tree_root || !node_traceback.empty()) {
        // Insert child node into the current node
        current_node->Insert(middle_key, child_node);

        // Overflow
        if (current_node->IsOverflow()) {
          Node* new_node = current_node->Split();
          middle_key = dynamic_cast<InnerNode*>(new_node)->RemoveFirstKey();

          // Context we have is the left node
          // new_node is the right node
          if (current_node == *tree_root) {
            TERRIER_ASSERT(node_traceback.empty(), "Stack should be empty when current node is the root");

            auto new_root = new InnerNode();
            // Will basically insert the key and node as node is empty
            new_root->Insert(middle_key, new_node);
            new_root->SetPrevPtr(current_node);
            *tree_root = new_root;
            return;
          }

          // Update current_node to the parent
          current_node = node_traceback.top();
          node_traceback.pop();
          child_node = new_node;
        }
        else {
          // Insertion does not cause overflow
          break;
        }
      }
    }

    Node* Split() {
      InnerNode* new_node = new InnerNode();

      // Copy the right half entries into the new node
      new_node->Copy(entries.begin() + MIN_KEYS_INNER_NODE, entries.end());

      // Delete the entries in the right half of the current node
      entries.erase(entries.begin() + MIN_KEYS_INNER_NODE, entries.end());

      return new_node;
    }

    void Copy(typename std::vector<KeyNodePtrPair>::iterator begin,
              typename std::vector<KeyNodePtrPair>::iterator end) {
      entries.insert(entries.end(), begin, end);
    }

    KeyType RemoveFirstKey() {
      prev_ptr = entries[0].second;
      KeyType first_key = entries[0].first;
      entries.erase(entries.begin());
      return first_key;
    }

    bool isLeaf() {
      return false;
    }

    Node* GetNodePtrForKey(const KeyType& key) {
      if(key_cmp_obj(key, (entries.begin())->first)) {
        return prev_ptr;
      }

      for (auto it = entries.begin(); it+1 != entries.end(); it++) {
        if (!key_cmp_obj(key, it->first) && key_cmp_obj(key, (it+1)->first)) {
          return it->second;
        }
      }

      return (entries.rbegin())->second;
    }

    size_t GetHeapSpaceSubtree() {
      size_t size = 0;

      TERRIER_ASSERT(prev_ptr != nullptr, "There shouldn't be a node without prev ptr");
      // Space for subtree pointed to by previous
      size += prev_ptr->GetHeapSpaceSubtree();

      // Current node's heap space used
      size += (entries.capacity()) * sizeof(KeyNodePtrPair);

      // For all children
      for (auto it = entries.begin(); it != entries.end(); ++it) {
        size += (it->second)->GetHeapSpaceSubtree();
      }

      return size;
    }
  };

  LeafNode* FindLeafNode(const KeyType& key, std::stack<InnerNode*>& node_traceback) {
    Node* node = root;

    while (!node->isLeaf()) {
      InnerNode* inner_node = dynamic_cast<InnerNode*>(node);
      node_traceback.push(inner_node);
      node = inner_node->GetNodePtrForKey(key);
    }

    return dynamic_cast<LeafNode*>(node);
  }

  LeafNode* FindLeafNode(const KeyType& key) {
    Node* node = root;

    while (node != nullptr && !node->isLeaf()) {
      InnerNode* inner_node = dynamic_cast<InnerNode*>(node);
      node = inner_node->GetNodePtrForKey(key);
    }

    return dynamic_cast<LeafNode*>(node);
  }

  void InsertAndPropagate(const KeyType &key, const ValueType &value, LeafNode* insert_node,
      std::stack<InnerNode*> &node_traceback) {
    insert_node->Insert(key, value);
    // If insertion causes overflow
    if (insert_node->IsOverflow()) {
      // Split the insert node and return the new (right) node with all values updated
      LeafNode* child_node = dynamic_cast<LeafNode*>(insert_node->Split());

      // insert_node : left
      // child_node : right
      if (insert_node == root) {
        auto new_root = new InnerNode();
        new_root->Insert(child_node->GetFirstKey(), child_node);
        new_root->SetPrevPtr(insert_node);
        root = dynamic_cast<Node*>(new_root);
        return;
      }

      InnerNode* parent_node = dynamic_cast<InnerNode*> (node_traceback.top());
      node_traceback.pop();

      // Insert the leaf node at the inner node and propagate
      parent_node->InsertNodePtr(child_node, &root, node_traceback);
    }
  }

 public:
  BPlusTree() {
    root = nullptr;
  }

  Node* GetRoot() {
    return root;
  }

  bool Insert(const KeyType &key,const ValueType &value, bool unique_key = false) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);
    std::stack<InnerNode*> node_traceback;
    LeafNode* insert_node;

    // If tree is empty
    if (root == nullptr) {
      root = new LeafNode();
      insert_node = dynamic_cast<LeafNode*>(root);
    } else {
      insert_node = FindLeafNode(key, node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->HasKeyValue(key, value) || (unique_key && insert_node->HasKey(key))) {
      return false;  // The traverse function aborts the insert as key, val is present
    }

    InsertAndPropagate(key, value, insert_node, node_traceback);

    return true;
  }

  bool ConditionalInsert(const KeyType &key, const ValueType &value, std::function<bool(const ValueType)> predicate,
                         bool *predicate_satisfied) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);
    LeafNode* insert_node;
    std::stack<InnerNode*> node_traceback;

    // If tree is empty
    if (root == nullptr) {
      insert_node = new LeafNode();
      root = insert_node;
    } else {
      insert_node = FindLeafNode(key, node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->SatisfiesPredicate(key, predicate)) {
      *predicate_satisfied = true;
      return false;  // The traverse function aborts the insert as key, val is present
    } else {
      *predicate_satisfied = false;
    }

    InsertAndPropagate(key, value, insert_node, node_traceback);

    return true;
  }

  void GetValue(const KeyType &key, typename std::vector<ValueType> &results) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);

    LeafNode* node = FindLeafNode(key);

    if (node == nullptr) {
      return;
    }

    node->ScanAndPopulateResults(key, results);
  }

  size_t GetHeapUsage() {
    if (root == nullptr) {
      return 0;
    }

    return root->GetHeapSpaceSubtree();
  }
};
}  // namespace terrier::storage::index