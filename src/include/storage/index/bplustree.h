#pragma once

#include <functional>
#include <iterator>
#include <stack>
#include <unordered_set>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "common/spin_latch.h"

namespace terrier::storage::index {

#define FAN_OUT 10
// Ceil (FAN_OUT / 2) - 1
#define MIN_KEYS_INNER_NODE 4
#define MIN_PTR_INNER_NODE 5
// Ceil ((FAN_OUT - 1) / 2)
#define MIN_KEYS_LEAF_NODE 5

/*
 * BPlusTree - Implementation of a B+ Tree index
 *
 * Template Arguments:
 *
 * template <typename KeyType,
 *           typename ValueType,
 *           typename KeyComparator = std::less<KeyType>,
 *           typename KeyEqualityChecker = std::equal_to<KeyType>,
 *           typename KeyHashFunc = std::hash<KeyType>,
 *           typename ValueEqualityChecker = std::equal_to<ValueType>,
 *           typename ValueHashFunc = std::hash<ValueType>>
 *
 * Explanation:
 *
 *  - KeyType: Key type of the map
 *
 *  - ValueType: Value type of the map. Note that it is possible
 *               that a single key is mapped to multiple values
 *
 *  - KeyComparator: "less than" relation comparator for KeyType
 *                   Returns true if "less than" relation holds
 *                   *** NOTE: THIS OBJECT DO NOT NEED TO HAVE A DEFAULT
 *                   CONSTRUCTOR.
 *                   Please refer to main.cpp, class KeyComparator for more
 *                   information on how to define a proper key comparator
 *
 *  - KeyEqualityChecker: Equality checker for KeyType
 *                        Returns true if two keys are equal
 *
 *  - KeyHashFunc: Hashes KeyType into size_t. This is used in unordered_set
 *
 *  - ValueEqualityChecker: Equality checker for value type
 *                          Returns true for ValueTypes that are equal
 *
 *  - ValueHashFunc: Hashes ValueType into a size_t
 *                   This is used in unordered_set
 */
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
    virtual ~Node() = default;

    virtual Node *Split() = 0;
    virtual void SetPrevPtr(Node *ptr) = 0;
    virtual bool IsLeaf() = 0;
    virtual uint64_t GetSize() = 0;
    virtual size_t GetHeapSpaceSubtree() = 0;
    virtual Node *GetPrevPtr() = 0;
    virtual KeyType GetFirstKey() = 0;
    virtual void Append(Node* node) = 0;
  };

  // Root of the tree
  Node *root_;
  // Datatypes for representing Node contents
  using ValueSet = std::unordered_set<ValueType, ValueHashFunc, ValueEqualityChecker>;
  using KeyValueSetPair = std::pair<KeyType, ValueSet>;
  using KeyNodePtrPair = std::pair<KeyType, Node *>;

  /*
   * LeafNode represents the leaf in the B+ Tree, storing the actual values in the index
   */
  class LeafNode : public Node {
   private:
    friend class BPlusTree;

    // Each key has a list of values stored as an unordered set
    std::vector<KeyValueSetPair> entries_;
    // Sibling pointers
    LeafNode *prev_ptr_;
    LeafNode *next_ptr_;

   public:
    LeafNode() {
      prev_ptr_ = nullptr;
      next_ptr_ = nullptr;
    }

    ~LeafNode() {
      entries_.clear();
      prev_ptr_ = nullptr;
      next_ptr_ = nullptr;
    }

    // Find the sorted position for a new key
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

    // Returns the last index whose key less than or equal to the given key
    // TODO(abhijithanilkumar): Optimize and use binary search
    uint64_t GetPositionLessThanEqualTo(const KeyType &key) {
      int i;

      for (i = 0; i < entries_.size(); i++) {
        if (KEY_CMP_OBJ(key, entries_[i].first)) {
          break;
        }
      }

      return (i - 1);
    }

    // Return an iterator to the position of the key
    typename std::vector<KeyValueSetPair>::iterator GetPositionOfKey(const KeyType &key) {
      auto it = entries_.begin();

      while (it != entries_.end()) {
        if (KEY_EQ_CHK(it->first, key)) return it;
        it++;
      }

      return it;
    }


    // Returns the prev_ptr for the node
    Node *GetPrevPtr() override { return prev_ptr_; }

    // Check if the given node has overflown
    bool IsOverflow() {
      uint64_t size = entries_.size();
      return (size >= FAN_OUT);
    }

    // Check if the given node has underflown
    bool IsUnderflow() {
      uint64_t size = entries_.size();
      return (size < MIN_KEYS_LEAF_NODE);
    }

    // Check if the given node will underflow after deletion
    bool WillUnderflow() {
      uint64_t size = entries_.size();
      return ((size - 1) < MIN_KEYS_LEAF_NODE);
    }

    // Set the previous pointer for the leaf node
    void SetPrevPtr(Node *ptr) override { prev_ptr_ = dynamic_cast<LeafNode *>(ptr); }

    // Check if the given key is present in the node
    bool HasKey(const KeyType &key) {
      // TODO(abhijithanilkumar): Optimize using STL function
      for (auto it = entries_.begin(); it != entries_.end(); it++) {
        if (KEY_EQ_CHK(it->first, key)) return true;
      }

      return false;
    }

    // Check if the given (key, value) pair is present in the node
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

    // Insert a new (key, value) pair into the leaf node
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

    void Insert(const KeyType &key, const ValueSet &value_set) {
      uint64_t pos_to_insert = GetPositionToInsert(key);

      if (pos_to_insert < entries_.size() && KEY_EQ_CHK(entries_[pos_to_insert].first, key)) {
        entries_[pos_to_insert].second = value_set;
      } else {
        KeyValueSetPair new_pair;
        new_pair.first = key;
        new_pair.second = value_set;
        entries_.insert(entries_.begin() + pos_to_insert, new_pair);
      }
    }

    // Returns the first key in the leaf node
    KeyType GetFirstKey() override { return entries_[0].first; }

    // Split the node into two, return the new node and set sibling pointers
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

    // Used to copy new values into the leaf node
    void Copy(typename std::vector<KeyValueSetPair>::iterator begin,
              typename std::vector<KeyValueSetPair>::iterator end) {
      entries_.insert(entries_.end(), begin, end);
    }

    // Check if values in a key satisfies predicate, used for Conditional Insert
    bool SatisfiesPredicate(const KeyType &key, std::function<bool(const ValueType)> predicate) {
      auto it = GetPositionOfKey(key);

      if (it == entries_.end()) return false;

      for (auto i = (it->second).begin(); i != (it->second).end(); i++) {
        if (predicate(*i)) return true;
      }

      return false;
    }

    // Returns true, since this is a leaf node
    bool IsLeaf() override { return true; }

    // Returns size (number of keys) in the node
    uint64_t GetSize() override { return entries_.size(); }

    // Populate the values in a key to a vector
    void ScanAndPopulateResults(const KeyType &key, typename std::vector<ValueType> *results) {
      auto it = GetPositionOfKey(key);

      if (it == entries_.end()) return;

      for (auto i = (it->second).begin(); i != (it->second).end(); i++) {
        results->push_back(*i);
      }
    }

    // Calculate the heap usage of the leaf node
    size_t GetHeapSpaceSubtree() override {
      size_t size = 0;
      // Current node's heap space used
      for (auto it = entries_.begin(); it != entries_.end(); ++it) {
        size += (it->second).size() * sizeof(ValueType) + sizeof(KeyType);
      }
      return size;
    }

    // Return the begin() iterator for entries_ in the node
    typename std::vector<KeyValueSetPair>::iterator GetEntriesBegin() {
      return entries_.begin();
    }

    // Return the end() iterator for entries_ in the node
    typename std::vector<KeyValueSetPair>::iterator GetEntriesEnd() {
      return entries_.end();
    }

    // Append the entries in the node passed to the current node
    void Append (Node *node) override {
      TERRIER_ASSERT(node->IsLeaf(), "Node passed has to be a leaf.");
      auto node_ptr = dynamic_cast<LeafNode*>(node);
      entries_.insert(entries_.end(), node_ptr->GetEntriesBegin(), node_ptr->GetEntriesEnd());
    }

    // Remove the last (key, val) pair from the node and return it
    KeyValueSetPair  RemoveLastKeyValPair() {
      auto last_key_val_pair = *entries_.rbegin();
      entries_.erase(entries_.end() - 1);
      return  last_key_val_pair;
    }

    // Remove the first (key, val) pair from the node and return it
    KeyValueSetPair  RemoveFirstKeyValPair() {
      auto first_key_val_pair = *entries_.begin();
      entries_.erase(entries_.begin());
      return first_key_val_pair;
    }

    // Delete the corresponding (key, value) entry from the node
    void DeleteEntry(const KeyType &key, const ValueType &value) {
      auto key_iter = GetPositionOfKey(key);
      auto value_iter = (key_iter->second).find(value);
      (key_iter->second).erase(value_iter);

      if ((key_iter->second).empty()) {
        entries_.erase(key_iter);
      }
      return;
    }

    LeafNode *GetNextPtr() {
      return next_ptr_;
    }

    void SetNextPtr(LeafNode *node) {
      next_ptr_ = node;
    }
  };

  /*
   * InnerNode represents one of the nodes in the non-leaf levels of the B+ Tree.
   */
  class InnerNode : public Node {
   private:
    friend class BPlusTree;
    // Node contains a vector of (key, ptr) pairs
    std::vector<KeyNodePtrPair> entries_;

    // n pointers, n-1 keys
    Node *prev_ptr_;

   public:
    InnerNode() { prev_ptr_ = nullptr; }

    ~InnerNode() {
      entries_.clear();
      prev_ptr_ = nullptr;
    }

    // Returns the prev_ptr
    Node *GetPrevPtr() override { return prev_ptr_; }

    // Returns the first index whose key greater than or equal to the given key
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

    // Returns the last index whose key less than or equal to the given key
    // TODO(abhijithanilkumar): Optimize and use binary search
    uint64_t GetPositionLessThanEqualTo(const KeyType &key) {
      int i;

      for (i = 0; i < entries_.size(); i++) {
        if (KEY_CMP_OBJ(key, entries_[i].first)) {
          break;
        }
      }

      return (i - 1);
    }

    // Returns size (number of keys) in the node
    uint64_t GetSize() override { return entries_.size(); }

    // Check if the node has overflown
    // Assumption: prev_ptr_ is occupied, hence the check is >= FAN_OUT
    bool IsOverflow() {
      uint64_t size = entries_.size();
      return (size >= FAN_OUT);
    }

    // Check if the given node has underflow
    bool IsUnderflow() {
      // Function maybe called after deleting prev_ptr_
      uint64_t size = entries_.size() + (prev_ptr_ != nullptr);
      return (size < MIN_PTR_INNER_NODE);
    }

    // Check if the given node will underflow after deletion
    // Assumption: Used for borrowing, only looks at no. of keys
    bool WillUnderflow() {
      // Adding 1 assuming that prev_ptr_ is always occupied
      uint64_t size = entries_.size() + 1;
      return ((size - 1) < MIN_PTR_INNER_NODE);
    }

    // Returns the predecessor of the node that has the given key
    Node* GetPredecessor(const KeyType &key) {
      uint64_t index = GetPositionLessThanEqualTo(key);

      // If your index is -1, you do not have a predecessor
      // Also means that the given key is pointed to by prev_ptr_
      if (index == -1) return nullptr;

      uint64_t pred_index = index - 1;

      // The predecessor is pointed to by prev_ptr_
      if (pred_index == -1) return prev_ptr_;

      return entries_[pred_index].second;
    }

    // Returns the successor of the node that has the given key
    Node* GetSuccessor(const KeyType &key) {
      uint64_t index = GetPositionLessThanEqualTo(key);

      uint64_t succ_index = index + 1;

      // The predecessor is pointed to by prev_ptr_
      if (succ_index == entries_.size()) return nullptr;

      return entries_[succ_index].second;
    }

    // Return the begin() iterator for entries_ in the node
    typename std::vector<KeyNodePtrPair>::iterator GetEntriesBegin() {
      return entries_.begin();
    }

    // Return the end() iterator for entries_ in the node
    typename std::vector<KeyNodePtrPair>::iterator GetEntriesEnd() {
      return entries_.end();
    }

    // Append the entries in the node passed to the current node
    void Append (Node *node) override {
      TERRIER_ASSERT(!node->IsLeaf(), "Node passed has to be an inner node.");
      auto node_ptr = dynamic_cast<InnerNode*>(node);
      entries_.insert(entries_.end(), node_ptr->GetEntriesBegin(), node_ptr->GetEntriesEnd());
    }

    // Insert a new (key, ptr) pair into the node
    void Insert(const KeyType &key, Node *node_ptr) {
      uint64_t pos_to_insert = GetPositionGreaterThanEqualTo(key);

      // We are sure that there is no duplicate key
      KeyNodePtrPair new_pair;
      new_pair.first = key;
      new_pair.second = node_ptr;
      entries_.insert(entries_.begin() + pos_to_insert, new_pair);
    }

    // Set pre_ptr for the inner node (this will point to a node in the lower level)
    void SetPrevPtr(Node *node) override { prev_ptr_ = node; }

    // Insert the new node appropriately in the tree, propagating the changes up till root if necessary
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

    // Split the inner node and return the new node created
    Node *Split() override {
      auto new_node = new InnerNode();

      // Copy the right half entries_ into the new node
      new_node->Copy(entries_.begin() + MIN_KEYS_INNER_NODE, entries_.end());

      // Delete the entries_ in the right half of the current node
      entries_.erase(entries_.begin() + MIN_KEYS_INNER_NODE, entries_.end());

      return new_node;
    }

    // Used to copy new values into the node
    void Copy(typename std::vector<KeyNodePtrPair>::iterator begin,
              typename std::vector<KeyNodePtrPair>::iterator end) {
      entries_.insert(entries_.end(), begin, end);
    }

    // Removes the first element in the node, used during insert overflow
    KeyType RemoveFirstKey() {
      prev_ptr_ = entries_[0].second;
      KeyType first_key = entries_[0].first;
      entries_.erase(entries_.begin());
      return first_key;
    }

    // Returns false because this is an inner node
    bool IsLeaf() override { return false; }

    // Returns the (key, ptr) pair iterator corresponding to the key
    typename std::vector<KeyNodePtrPair>::iterator GetKeyNodePtrPair(const KeyType &key) {
      for (auto it = entries_.begin(); it + 1 != entries_.end(); it++) {
        if (!KEY_CMP_OBJ(key, it->first) && KEY_CMP_OBJ(key, (it + 1)->first)) {
          return it;
        }
      }

      return (entries_.end() - 1);
    }

    // Returns the pointer corresponding to the key, used to traverse
    Node *GetNodePtrForKey(const KeyType &key) {
      if (KEY_CMP_OBJ(key, (entries_.begin())->first)) {
        return prev_ptr_;
      }

      auto key_ptr_iter = GetKeyNodePtrPair(key);
      return key_ptr_iter->second;
    }

    // Calculate the space used by the subtree starting at this node
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

    // Get the first key in the node
    KeyType GetFirstKey() override { return entries_[0].first; }

    // Replace the key that points to the old_key with new_key
    KeyType ReplaceKey(const KeyType &old_key, const KeyType &new_key) {
      uint64_t key_pos = GetPositionLessThanEqualTo(old_key);
      KeyType old_parent_key = entries_[key_pos].first;
      entries_[key_pos].first = new_key;

      return old_parent_key;
    }

    // Remove the last (key, ptr) pair from the node and return it
    KeyNodePtrPair RemoveLastKeyNodePtrPair() {
      auto last_key_ptr_pair = *entries_.rbegin();
      entries_.erase(entries_.end() - 1);
      return last_key_ptr_pair;
    }

    // Remove the first (key, ptr) pair from the node and return it
    KeyNodePtrPair RemoveFirstKeyNodePtrPair() {
      auto first_key_ptr_pair = *entries_.begin();
      entries_.erase(entries_.begin());
      return first_key_ptr_pair;
    }

    // Delete the corresponding (key, value) entry from the node
    KeyType DeleteEntry(const KeyType &key) {
      auto key_iter = GetKeyNodePtrPair(key);
      KeyType deleted_key = key_iter->first;
      entries_.erase(key_iter);
      return deleted_key;
    }
  };

  class IndexIterator {
    LeafNode* current_;
    int key_offset_;
    int value_offset_;

    public:
     KeyType first;
     ValueType second;

     IndexIterator(LeafNode* c, size_t k, size_t v) {
       current_ = c;
       key_offset_ = k;
       value_offset_ = v;
       if (current_ != nullptr) {
         auto key_val_iter = (current_->GetEntriesBegin() + key_offset_);
         first = key_val_iter->first;
         second = *(std::next(key_val_iter->second.begin(), value_offset_));
       }
     }

     IndexIterator(const IndexIterator& itr) {
       current_ = itr.current_;
       key_offset_ = itr.key_offset_;
       value_offset_ = itr.value_offset_;
       first = itr.first;
       second = itr.second;
     }

     bool operator==(const IndexIterator& itr) {
        return (current_ == itr.current_ &&
                key_offset_ == itr.key_offset_ &&
                value_offset_ == itr.value_offset_);
     }

     void operator++() {
       if (key_offset_ < current_->GetSize() - 1) {
         if (value_offset_ < (current_->GetEntriesBegin() + key_offset_)->second.size() - 1) {
           value_offset_++;
         } else {
           key_offset_++;
           value_offset_ = 0;
         }
       } else {
         if (value_offset_ < (current_->GetEntriesBegin() + key_offset_)->second.size() - 1) {
           value_offset_++;
         } else {
           TERRIER_ASSERT(current_ != nullptr, "The ++ operator should not be called for a null iterator");
           current_ = current_->GetNextPtr();
           key_offset_ = 0;
           value_offset_ = 0;
         }
       }
       if (current_ != nullptr) {
         auto key_val_iter = (current_->GetEntriesBegin() + key_offset_);
         first = key_val_iter->first;
         second = *(std::next(key_val_iter->second.begin(), value_offset_));
       }
     }

     void operator--() {
       if (key_offset_ > 0) {
         if (value_offset_ > 0) {
           value_offset_--;
         } else {
           key_offset_--;
           value_offset_ = (current_->GetEntriesBegin() + key_offset_)->second.size() - 1;
         }
       } else {
         if (value_offset_ > 0) {
           value_offset_--;
         } else {
             TERRIER_ASSERT(current_ != nullptr, "The -- operator should not be called for a null iterator");
             current_ = dynamic_cast<LeafNode *>(current_->GetPrevPtr());
             if (current_ != nullptr) {
               key_offset_ = current_->GetSize() - 1;
               value_offset_ = (current_->GetEntriesBegin() + key_offset_)->second.size() - 1;
             } else {
               key_offset_ = 0;
               value_offset_ = 0;
             }
         }
       }
       if (current_ != nullptr) {
         auto key_val_iter = (current_->GetEntriesBegin() + key_offset_);
         first = key_val_iter->first;
         second = *(std::next(key_val_iter->second.begin(), value_offset_));
       }
     }
  };

  // Traverse and find the leaf node that has the given key, populate the stack to store the path
  LeafNode *FindLeafNode(const KeyType &key, std::stack<InnerNode *> *node_traceback) {
    Node *node = root_;

    while (!node->IsLeaf()) {
      auto inner_node = dynamic_cast<InnerNode *>(node);
      node_traceback->push(inner_node);
      node = inner_node->GetNodePtrForKey(key);
    }

    return dynamic_cast<LeafNode *>(node);
  }

  // Traverse the tree to find the leaf node that has the given key
  LeafNode *FindLeafNode(const KeyType &key) {
    Node *node = root_;

    while (node != nullptr && !node->IsLeaf()) {
      auto inner_node = dynamic_cast<InnerNode *>(node);
      node = inner_node->GetNodePtrForKey(key);
    }

    return dynamic_cast<LeafNode *>(node);
  }

  // Insert a new (key, value) pair in the tree and rebalance the tree
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

  void BorrowFromLeftLeaf(LeafNode *left_sibling, LeafNode *node, InnerNode *parent) {
    KeyValueSetPair last_key_val_pair = left_sibling->RemoveLastKeyValPair();

    // GetFirstKey() might not be present in the parent node, but we replace the corresponding key
    parent->ReplaceKey(node->GetFirstKey(), last_key_val_pair.first);

    node->Insert(last_key_val_pair.first, last_key_val_pair.second);
  }

  void BorrowFromRightLeaf(LeafNode *right_sibling, LeafNode *node, InnerNode *parent) {
    KeyValueSetPair first_key_val_pair = right_sibling->RemoveFirstKeyValPair();

    // The key might not be present in the parent node, but we replace the corresponding key
    parent->ReplaceKey(first_key_val_pair.first, right_sibling->GetFirstKey());

    node->Insert(first_key_val_pair.first, first_key_val_pair.second);
  }

  void BorrowFromLeftInner(InnerNode *left_sibling, InnerNode *node, InnerNode *parent) {
    KeyNodePtrPair last_key_node_pair = left_sibling->RemoveLastKeyNodePtrPair();

    // GetFirstKey() might not be present in the parent node, but we replace the corresponding key
    KeyType old_parent_key = parent->ReplaceKey(node->GetFirstKey(), last_key_node_pair.first);

    node->Insert(old_parent_key, node->GetPrevPtr());

    node->SetPrevPtr(last_key_node_pair.second);
  }

  void BorrowFromRightInner(InnerNode *right_sibling, InnerNode *node, InnerNode *parent) {
    // Remove first key value pair (key is transferred to parent, Node pointer becomes new prev pointer)
    KeyNodePtrPair first_key_node_pair = right_sibling->RemoveFirstKeyNodePtrPair();

    // Replace the key in the parent with the next lowest key in the right subtree
    KeyType old_parent_key = parent->ReplaceKey(first_key_node_pair.first, first_key_node_pair.first);

    node->Insert(old_parent_key, right_sibling->GetPrevPtr());
    right_sibling->SetPrevPtr(first_key_node_pair.second);
  }

  // Coalesce from source to destination (right to left)
  void CoalesceLeaf(LeafNode *src, LeafNode *dst, InnerNode *parent) {
    // Both src and dst of same level

    // Deletes the entry pointing to src node
    parent->DeleteEntry(src->GetFirstKey());

    // Copy entries
    dst->Append(src);
  }

  // Coalesce from source to destination (right to left)
  void CoalesceInner(InnerNode *src, InnerNode *dst, InnerNode *parent) {
    // Both src and dst of same level
    // Deletes the entry pointing to src node
    KeyType parent_key = parent->DeleteEntry(src->GetFirstKey());

    dst->Insert(parent_key, src->GetPrevPtr());

    // Copy entries
    dst->Append(src);
  }

  // Balance a tree on deletion at leaf node
  void Balance(LeafNode *node, std::stack<InnerNode *> *node_traceback) {
    // Handle leaf merge separately
    auto parent_node = node_traceback->top();
    auto left_sibling = dynamic_cast<LeafNode *>(parent_node->GetPredecessor(node->GetFirstKey()));
    auto right_sibling = dynamic_cast<LeafNode *>(parent_node->GetSuccessor(node->GetFirstKey()));

    // Borrow from siblings if possible
    if (left_sibling && !left_sibling->WillUnderflow()) {
      BorrowFromLeftLeaf(left_sibling, node, parent_node);
      return;
    }
    if (right_sibling && !right_sibling->WillUnderflow()) {
      BorrowFromRightLeaf(right_sibling, node, parent_node);
      return;
    }

    // Try Coalesce
    // Parent node entry for child is also deleted
    if (left_sibling) {
      CoalesceLeaf(node, left_sibling, parent_node);
      left_sibling->SetNextPtr(node->GetNextPtr());
      if (node->GetNextPtr() != nullptr) {
        node->GetNextPtr()->SetPrevPtr(left_sibling);
      }
      delete node;
    } else {
      CoalesceLeaf(right_sibling, node, parent_node);
      node->SetNextPtr(right_sibling->GetNextPtr());
      if (right_sibling->GetNextPtr() != nullptr) {
        right_sibling->GetNextPtr()->SetPrevPtr(node);
      }
      delete right_sibling;
    }

    node_traceback->pop();

    // Handle inner nodes now
    auto inner_node = parent_node;

    while (inner_node == root_ || inner_node->IsUnderflow()) {
      if (inner_node == root_) {
        if (inner_node->GetSize() == 0) {
          // Only 1 pointer left in the root
          auto tmp = root_;
          root_ = root_->GetPrevPtr();
          delete tmp;
        }
        return;
      }

      parent_node = node_traceback->top();
      node_traceback->pop();
      auto left_inner = dynamic_cast<InnerNode *>(parent_node->GetPredecessor(inner_node->GetFirstKey()));
      auto right_inner = dynamic_cast<InnerNode *>(parent_node->GetSuccessor(inner_node->GetFirstKey()));

      if (left_inner && !left_inner->WillUnderflow()) {
        BorrowFromLeftInner(left_inner, inner_node, parent_node);
        return;
      }
      if (right_inner && !right_inner->WillUnderflow()) {
        BorrowFromRightInner(right_inner, inner_node, parent_node);
        return;
      }

      // Try Coalesce
      // Parent node entry for child is also deleted
      if (left_inner) {
        CoalesceInner(inner_node, left_inner, parent_node);
        delete inner_node;
      } else {
        CoalesceInner(right_inner, inner_node, parent_node);
        delete right_inner;
      }

      inner_node = parent_node;
    }
  }

 public:
  BPlusTree() { root_ = nullptr; }

  // Returns the root of the B+ tree
  Node *GetRoot() { return root_; }

  // API to insert a new (key, value) pair into the tree
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

  // API to perform (key, value) pair insert based on a predicate into the tree
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

  // API to fetch the values stored in the corresponding key and populate a vector with it
  void GetValue(const KeyType &key, typename std::vector<ValueType> *results) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);

    LeafNode *node = FindLeafNode(key);

    if (node == nullptr) {
      return;
    }

    node->ScanAndPopulateResults(key, results);
  }

  // API to calculate heap usage
  size_t GetHeapUsage() {
    if (root_ == nullptr) {
      return 0;
    }

    return root_->GetHeapSpaceSubtree();
  }

  // API to get the height of the tree
  size_t GetHeightOfTree() {
    size_t height = 1;

    Node *node = root_;

    if (node == nullptr) return 0;

    while (!node->IsLeaf()) {
      height++;
      node = node->GetPrevPtr();
    }

    return height;
  }

  // API to delete an entry in the tree
  bool Delete(const KeyType &key, const ValueType &value) {
    // Avoid races
    common::SpinLatch::ScopedSpinLatch guard(&tree_latch_);

    if (root_ == nullptr) return false;

    std::stack<InnerNode *> node_traceback;
    auto node = FindLeafNode(key, &node_traceback);

    if (!node->HasKeyValue(key, value)) {return false;}

    // Delete entry for leaf node
    node->DeleteEntry(key, value);

    if (node == root_){
      // Root is empty
      if (node->GetSize() == 0) {
        delete root_;
        root_ = nullptr;
      }
      return true;
    }

    // Must propagate
    if (node->IsUnderflow()) {
      // Balance at leaf level
      Balance(node, &node_traceback);
    }

    return true;
  }

  IndexIterator begin() {
    Node *node = root_;
    while (!node->IsLeaf()) {
      node = node->GetPrevPtr();
    }
    return IndexIterator(dynamic_cast<LeafNode *>(node), 0, 0);
  }

  IndexIterator begin(const KeyType &key) {
    auto node = FindLeafNode(key);
    auto pos = node->GetPositionToInsert(key);
    return IndexIterator(node, pos, 0);
  }

  IndexIterator end() { return IndexIterator(nullptr, 0, 0); }

  IndexIterator end(const KeyType &key) {
    auto node = FindLeafNode(key);
    auto pos = node->GetPositionLessThanEqualTo(key);
    if (pos == -1) {
      node = dynamic_cast<LeafNode *>(node->GetPrevPtr());
      if (node == nullptr) {
        return IndexIterator(node, 0, 0);
      }
      pos = node->GetSize() - 1;
    }
    int val_off = (node->GetEntriesEnd() - 1)->second.size() - 1;
    return IndexIterator(node, pos, val_off);
  }

  bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) {
    return KEY_CMP_OBJ(key2, key1);
  }

  bool KeyCmpGreaterEqual(const KeyType &key1, const KeyType &key2) {
    return !KEY_CMP_OBJ(key1, key2);
  }
};
}  // namespace terrier::storage::index
