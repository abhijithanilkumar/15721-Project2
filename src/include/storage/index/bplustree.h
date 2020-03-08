#pragma once

#include <functional>
#include <iterator>
#include <unordered_set>
#include <stack>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "common/spin_latch.h"

namespace terrier::storage::index {

#define FAN_OUT 10
// Ceil (FAN_OUT / 2) - 1
#define MIN_KEYS_INNER_NODE 4
// Ceil ((FAN_OUT - 1) / 2)
#define MIN_KEYS_LEAF_NODE 5

template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>, typename ValueHashFunc = std::hash<ValueType>>
class BPlusTree {
  // static definition of comparators and equality checkers
  constexpr static const KeyComparator KEY_CMP_OBJ{};
  constexpr static const KeyEqualityChecker KEY_EQ_CHK{};
  constexpr static const ValueEqualityChecker VAL_EQ_CHK{};
  // Global latch for the entire tree
  mutable common::SpinLatch tree_latch_;
  /*
   * class Node - The base class for node types, i.e. InnerNode and LeafNode
   *
   * Since for InnerNode and LeafNode, the number of elements is not a compile
   * time known constant.
   */
  class Node {
   public:
    Node() = default;
    ~Node() = default;

    virtual Node *Split() = 0;
    virtual void SetPrevPtr(Node *ptr) = 0;
    virtual bool IsLeaf() = 0;
    virtual size_t GetHeapSpaceSubtree() = 0;
    virtual Node* GetPrevPtr() = 0;
  };

  // Root of the tree
  Node *root_;
  using ValueSet = std::unordered_set<ValueType, ValueHashFunc, ValueEqualityChecker>;
  using KeyValueSetPair = std::pair<KeyType, ValueSet>;
  using KeyNodePtrPair = std::pair<KeyType, Node *>;

  class LeafNode : public Node {
   private:
    friend class BPlusTree;

    std::vector<KeyValueSetPair> entries_;
    // Sibling pointers
    LeafNode *prev_ptr_;
    LeafNode *next_ptr_;

    // TODO(abhijithanilkumar): Optimize and use binary search
    uint64_t GetPositionToInsert(const KeyType &key) {
      int i;

      for (i = 0; i < entries_.size(); i++) {
        if (!KEY_CMP_OBJ(entries_[i].first, key)) {
          break;
        }
      }

      return i;
    }

    typename std::vector<KeyValueSetPair>::iterator GetPositionOfKey(const KeyType &key) {
      auto it = entries_.begin();

      while (it != entries_.end()) {
        if (KEY_EQ_CHK(it->first, key)) return it;
        it++;
      }

      return it;
    }

   public:
    LeafNode() {
      prev_ptr_ = nullptr;
      next_ptr_ = nullptr;
    }

    ~LeafNode() = default;

    Node* GetPrevPtr() override {
      return prev_ptr_;
    }

    bool IsOverflow() {
      uint64_t size = entries_.size();
      return (size >= FAN_OUT);
    }

    void SetPrevPtr(Node *ptr) override { prev_ptr_ = dynamic_cast<LeafNode *>(ptr); }

    bool HasKey(const KeyType &key) {
      // TODO(abhijithanilkumar): Optimize using STL function
      for (auto it = entries_.begin(); it != entries_.end(); it++) {
        if (KEY_EQ_CHK(it->first, key)) return true;
      }

      return false;
    }

    bool HasKeyValue(const KeyType &key, const ValueType &value) {
      // TODO(abhijithanilkumar): Optimize using STL function
      for (auto it = entries_.begin(); it != entries_.end(); it++) {
        if (KEY_EQ_CHK(it->first, key)) {
          auto loc = it->second.find(value);
          if (loc != it->second.end()) return true;
        }
      }

      return false;
    }

    void Insert(const KeyType &key, const ValueType &value) {
      uint64_t pos_to_insert = GetPositionToInsert(key);

      if (pos_to_insert < entries_.size() && KEY_EQ_CHK(entries_[pos_to_insert].first, key)) {
        entries_[pos_to_insert].second.insert(value);
      } else {
        KeyValueSetPair new_pair;
        new_pair.first = key;
        new_pair.second.insert(value);
        entries_.insert(entries_.begin() + pos_to_insert, new_pair);
      }
    }

    KeyType GetFirstKey() { return entries_[0].first; }

    Node *Split() override {
      auto new_node = new LeafNode();

      // Copy the right half entries_ to the next node
      new_node->Copy(entries_.begin() + MIN_KEYS_LEAF_NODE, entries_.end());

      // Erase the right half from the current node
      entries_.erase(entries_.begin() + MIN_KEYS_LEAF_NODE, entries_.end());

      // Set the forward sibling pointer of the current node
      next_ptr_ = new_node;

      // Set the backward sibling pointer of the new node
      new_node->SetPrevPtr(this);

      return new_node;
    }

    void Copy(typename std::vector<KeyValueSetPair>::iterator begin,
              typename std::vector<KeyValueSetPair>::iterator end) {
      entries_.insert(entries_.end(), begin, end);
    }

    bool SatisfiesPredicate(const KeyType &key, std::function<bool(const ValueType)> predicate) {
      auto it = GetPositionOfKey(key);

      if (it == entries_.end()) return false;

      for (auto i = (it->second).begin(); i != (it->second).end(); i++) {
        if (predicate(*i)) return true;
      }

      return false;
    }

    bool IsLeaf() override { return true; }

    void ScanAndPopulateResults(const KeyType &key, typename std::vector<ValueType> *results) {
      auto it = GetPositionOfKey(key);

      if (it == entries_.end()) return;

      for (auto i = (it->second).begin(); i != (it->second).end(); i++) {
        results->push_back(*i);
      }
    }

    size_t GetHeapSpaceSubtree() override {
      size_t size = 0;
      // Current node's heap space used
      for (auto it = entries_.begin(); it != entries_.end(); ++it) {
        size += (it->second).size() * sizeof(ValueType) + sizeof(KeyType);
      }
      return size;
    }
  };

  class InnerNode : public Node {
   private:
    std::vector<KeyNodePtrPair> entries_;
    friend class BPlusTree;

    // n pointers, n-1 keys
    Node *prev_ptr_;

   public:
    InnerNode() { prev_ptr_ = nullptr; }

    ~InnerNode() = default;

    Node* GetPrevPtr() override {
      return prev_ptr_;
    }

    // TODO(abhijithanilkumar): Optimize and use binary search
    uint64_t GetPositionGreaterThanEqualTo(const KeyType &key) {
      int i;

      for (i = 0; i < entries_.size(); i++) {
        if (!KEY_CMP_OBJ(entries_[i].first, key)) {
          break;
        }
      }

      return i;
    }

    bool IsOverflow() {
      uint64_t size = entries_.size();
      return (size >= FAN_OUT);
    }

    void Insert(const KeyType &key, Node *node_ptr) {
      uint64_t pos_to_insert = GetPositionGreaterThanEqualTo(key);

      // We are sure that there is no duplicate key
      KeyNodePtrPair new_pair;
      new_pair.first = key;
      new_pair.second = node_ptr;
      entries_.insert(entries_.begin() + pos_to_insert, new_pair);
    }

    void SetPrevPtr(Node *node) override { prev_ptr_ = node; }

    void InsertNodePtr(Node *child_node, Node **tree_root, std::stack<InnerNode *> *node_traceback) {
      InnerNode *current_node = this;
      // Since child_node is always a leaf node, we do not remove first key here
      TERRIER_ASSERT(child_node->IsLeaf(), "child_node has to be a leaf node");
      KeyType middle_key = dynamic_cast<LeafNode *>(child_node)->GetFirstKey();

      // If current node is the root or has a parent
      while (current_node == *tree_root || !node_traceback->empty()) {
        // Insert child node into the current node
        current_node->Insert(middle_key, child_node);

        // Overflow
        if (current_node->IsOverflow()) {
          Node *new_node = current_node->Split();
          middle_key = dynamic_cast<InnerNode *>(new_node)->RemoveFirstKey();

          // Context we have is the left node
          // new_node is the right node
          if (current_node == *tree_root) {
            TERRIER_ASSERT(node_traceback->empty(), "Stack should be empty when current node is the root");

            auto new_root = new InnerNode();
            // Will basically insert the key and node as node is empty
            new_root->Insert(middle_key, new_node);
            new_root->SetPrevPtr(current_node);
            *tree_root = new_root;
            return;
          }

          // Update current_node to the parent
          current_node = node_traceback->top();
          node_traceback->pop();
          child_node = new_node;
        } else {
          // Insertion does not cause overflow
          break;
        }
      }
    }

    Node *Split() override {
      auto new_node = new InnerNode();

      // Copy the right half entries_ into the new node
      new_node->Copy(entries_.begin() + MIN_KEYS_INNER_NODE, entries_.end());

      // Delete the entries_ in the right half of the current node
      entries_.erase(entries_.begin() + MIN_KEYS_INNER_NODE, entries_.end());

      return new_node;
    }

    void Copy(typename std::vector<KeyNodePtrPair>::iterator begin,
              typename std::vector<KeyNodePtrPair>::iterator end) {
      entries_.insert(entries_.end(), begin, end);
    }

    KeyType RemoveFirstKey() {
      prev_ptr_ = entries_[0].second;
      KeyType first_key = entries_[0].first;
      entries_.erase(entries_.begin());
      return first_key;
    }

    bool IsLeaf() override { return false; }

    Node *GetNodePtrForKey(const KeyType &key) {
      if (KEY_CMP_OBJ(key, (entries_.begin())->first)) {
        return prev_ptr_;
      }

      for (auto it = entries_.begin(); it + 1 != entries_.end(); it++) {
        if (!KEY_CMP_OBJ(key, it->first) && KEY_CMP_OBJ(key, (it + 1)->first)) {
          return it->second;
        }
      }

      return (entries_.rbegin())->second;
    }

    size_t GetHeapSpaceSubtree() override {
      size_t size = 0;

      TERRIER_ASSERT(prev_ptr_ != nullptr, "There shouldn't be a node without prev ptr");
      // Space for subtree pointed to by previous
      size += prev_ptr_->GetHeapSpaceSubtree();

      // Current node's heap space used
      size += (entries_.capacity()) * sizeof(KeyNodePtrPair);

      // For all children
      for (auto it = entries_.begin(); it != entries_.end(); ++it) {
        size += (it->second)->GetHeapSpaceSubtree();
      }

      return size;
    }
  };

  LeafNode *FindLeafNode(const KeyType &key, std::stack<InnerNode *> *node_traceback) {
    Node *node = root_;

    while (!node->IsLeaf()) {
      auto inner_node = dynamic_cast<InnerNode *>(node);
      node_traceback->push(inner_node);
      node = inner_node->GetNodePtrForKey(key);
    }

    return dynamic_cast<LeafNode *>(node);
  }

  LeafNode *FindLeafNode(const KeyType &key) {
    Node *node = root_;

    while (node != nullptr && !node->IsLeaf()) {
      auto inner_node = dynamic_cast<InnerNode *>(node);
      node = inner_node->GetNodePtrForKey(key);
    }

    return dynamic_cast<LeafNode *>(node);
  }

  void InsertAndPropagate(const KeyType &key, const ValueType &value, LeafNode *insert_node,
                          std::stack<InnerNode *> *node_traceback) {
    insert_node->Insert(key, value);
    // If insertion causes overflow
    if (insert_node->IsOverflow()) {
      // Split the insert node and return the new (right) node with all values updated
      auto child_node = dynamic_cast<LeafNode *>(insert_node->Split());

      // insert_node : left
      // child_node : right
      if (insert_node == root_) {
        auto new_root = new InnerNode();
        new_root->Insert(child_node->GetFirstKey(), child_node);
        new_root->SetPrevPtr(insert_node);
        root_ = dynamic_cast<Node *>(new_root);
        return;
      }

      auto parent_node = dynamic_cast<InnerNode *>(node_traceback->top());
      node_traceback->pop();

      // Insert the leaf node at the inner node and propagate
      parent_node->InsertNodePtr(child_node, &root_, node_traceback);
    }
  }

 public:
  BPlusTree() { root_ = nullptr; }

  Node *GetRoot() { return root_; }

  bool Insert(const KeyType &key, const ValueType &value, bool unique_key = false) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);
    std::stack<InnerNode *> node_traceback;
    LeafNode *insert_node;

    // If tree is empty
    if (root_ == nullptr) {
      root_ = new LeafNode();
      insert_node = dynamic_cast<LeafNode *>(root_);
    } else {
      insert_node = FindLeafNode(key, &node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->HasKeyValue(key, value) || (unique_key && insert_node->HasKey(key))) {
      return false;  // The traverse function aborts the insert as key, val is present
    }

    InsertAndPropagate(key, value, insert_node, &node_traceback);

    return true;
  }

  bool ConditionalInsert(const KeyType &key, const ValueType &value, std::function<bool(const ValueType)> predicate,
                         bool *predicate_satisfied) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);
    LeafNode *insert_node;
    std::stack<InnerNode *> node_traceback;

    // If tree is empty
    if (root_ == nullptr) {
      insert_node = new LeafNode();
      root_ = insert_node;
    } else {
      insert_node = FindLeafNode(key, &node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->SatisfiesPredicate(key, predicate)) {
      *predicate_satisfied = true;
      return false;  // The traverse function aborts the insert as key, val is present
    }

    *predicate_satisfied = false;
    InsertAndPropagate(key, value, insert_node, &node_traceback);

    return true;
  }

  void GetValue(const KeyType &key, typename std::vector<ValueType> *results) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);

    LeafNode *node = FindLeafNode(key);

    if (node == nullptr) {
      return;
    }

    node->ScanAndPopulateResults(key, results);
  }

  size_t GetHeapUsage() {
    if (root_ == nullptr) {
      return 0;
    }

    return root_->GetHeapSpaceSubtree();
  }

  size_t GetHeightOfTree() {
    size_t height = 1;

    Node* node = root_;

    if (node == NULL)
      return 0;

    while (!node->IsLeaf()) {
      height++;
      node = node->GetPrevPtr();
    }

    return height;
  }

};
}  // namespace terrier::storage::index
