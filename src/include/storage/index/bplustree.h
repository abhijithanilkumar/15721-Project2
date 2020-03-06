#pragma once

#include <functional>
#include "common/macros.h"
#include <list>
#include <vector>
#include <iterator>
#include <stack>



namespace terrier::storage::index {

#define FAN_OUT 10

template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
    typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
    typename ValueEqualityChecker = std::equal_to<ValueType>>
class BPlusTree {
  class KeyValuePairEqualityChecker;

  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType : short {
    // We separate leaf and inner into two different intervals
    // to make it possible for compiler to optimize
    InnerType = 0,

    // Only valid for inner
    InnerInsertType = 1,
    InnerDeleteType = 2,
    InnerSplitType = 3,
    InnerRemoveType = 4,
    InnerMergeType = 5,
    InnerAbortType = 6,  // Unconditional abort

    LeafStart = 7,

    // Data page type
    LeafType = 7,

    // Only valid for leaf
    LeafInsertType = 8,
    LeafSplitType = 9,
    LeafDeleteType = 10,
    LeafRemoveType = 11,
    LeafMergeType = 12,
  };


/*
  * class NodeMetaData - Holds node metadata in an object
  *
  * Node metadata includes a pointer to the range object, the depth
  * of the current delta chain (NOTE: If there is a merge node then the
  * depth is the sum of the length of its two children rather than
  * the larger one)
  *
  * Since we need to query for high key and low key in every step of
  * traversal down the tree (i.e. on each level we need to verify we
  * are on the correct node). It would be wasteful if we traverse down the
  * delta chain down to the bottom everytime to collect these metadata
  * therefore as an optimization we store them inside each delta node
  * and leaf/inner node to optimize for performance
  *
  * NOTE: We do not count node type as node metadata
  */
  class NodeMetaData {
   public:
    // The type of the node; this is forced to be represented as a short type
    NodeType type;

    // This is the depth of current delta chain
    // For merge delta node, the depth of the delta chain is the
    // sum of its two children
    short depth;

    // This counts the number of items alive inside the Node
    // when consolidating nodes, we use this piece of information
    // to reserve space for the new node
    int item_count;

    /*
     * Constructor
     */
    NodeMetaData(NodeType p_type, int p_depth,
                 int p_item_count)
        : type{p_type},
          depth{static_cast<short>(p_depth)},
          item_count{p_item_count} {}
  };

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
    NodeMetaData metadata;

   public:
    /*
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */
    Node(NodeType p_type, int p_depth, int p_item_count)
        : metadata(p_type, p_depth, p_item_count) {}

   public:

  };

  using KeyValueListPair = std::pair<KeyType, std::list<ValueType>>;
  using KeyNodePtrPair = std::pair<KeyType, Node*>;

  class LeafNode: public Node {
   private:
    std::vector<std::pair<KeyType, std::list<ValueType>>> entries;

    // TODO: Optimize and use binary search
    uint64_t GetPositionToInsert(const KeyType& key) {
      int i;

      for (i = 0; i < entries.size(); i++) {
        if (entries[i].first >= key) {
          break;
        }
      }

      return i;
    }

   public:
    bool WillOverflow() {
      uint64_t size = entries.size();
      return (size >= FAN_OUT - 1);
    }

    bool HasKey(const KeyType &key) {
      // TODO: Optimize using STL function
      for(auto it = entries.begin(); it != entries.end(); it++) {
        if (it->first == key) return  true;
      }

      return false;
    }

    bool HasKeyValue(const KeyType &key, const ValueType &value) {
      // TODO: Optimize using STL function
      for (auto it = entries.begin(); it != entries.end(); it++) {
        if (it->first == key && it->second == value) return true;
      }

      return false;
    }

    void Insert(const KeyType& key, const ValueType& value) {
      uint64_t pos_to_insert = GetPositionToInsert(key);

      if (pos_to_insert < entries.size() && entries[pos_to_insert].first == key) {
        entries[pos_to_insert].second.push_back(value);
      } else {
        std::pair<KeyType, std::list<ValueType>> new_pair;
        new_pair.first = key;
        new_pair.second.push_back(value);
        entries.insert(entries.begin() + pos_to_insert, new_pair);
      }
    }
  };

  class InnerNode: public Node {
   private:
    std::vector<KeyNodePtrPair> entries;

    // TODO: Optimize and use binary search
    uint64_t GetPositionToInsert(const KeyType& key) {
      int i;

      for (i = 0; i < entries.size(); i++) {
        if (entries[i].first >= key) {
          break;
        }
      }

      return i;
    }

   public:
    bool WillOverflow() {
      uint64_t size = entries.size();
      return (size >= FAN_OUT - 1);
    }

    void Insert(const KeyType& key, const Node* node_ptr) {
      uint64_t pos_to_insert = GetPositionToInsert(key);

      // We are sure that there is no duplicate key
      KeyNodePtrPair new_pair;
      new_pair.first = key;
      new_pair.second = node_ptr;
      entries.insert(entries.begin() + pos_to_insert, new_pair);
    }
  };

  // Root of the tree
  Node* root;

 public:
  void InsertAndPropagate(const KeyType &key, const ValueType &value, LeafNode* insert_node,
      std::stack<Node*> &node_traceback) {

    bool will_overflow = insert_node->WillOverflow();

    insert_node->Insert(key, value);
    // If insertion causes overflow
    if (will_overflow) {
      // create new leaf node and insert key, value
      LeafNode* new_node = new LeafNode();

      // Move half of entries to the new node and update sibling pointers
      insert_node->Split(new_node);

      Node* child_node = new_node;

      if (insert_node == root) {
        root = new InnerNode();
        root->InsertNodePtr(child_node, node_traceback);
        root->SetPrevPtr(insert_node);
        return;
      }

      InnerNode* parent_node = dynamic_cast<InnerNode*> (node_traceback.top());
      node_traceback.pop();

      // Insert the leaf node at the inner node and propagate
      parent_node->InsertNodePtr(child_node, node_traceback);
    }
  }

  bool Insert(const KeyType &key, const ValueType &value, bool unique_key) {
    std::stack<Node*> node_traceback;
    LeafNode* insert_node;

    // If tree is empty
    if (root == nullptr) {
      root = new LeafNode();
      insert_node = root;
    } else {
      insert_node = FindLeafNode(key, value, node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->HasKeyVal(key, value) || (unique_key && insert_node->HasKey(key))) {
      return false;  // The traverse function aborts the insert as key, val is present
    }

    InsertAndPropagate(key, value, insert_node, node_traceback);

    return true;
  }

  bool ConditionalInsert(const KeyType &key, const ValueType &value, std::function<bool(const ValueType)> predicate,
                         bool *predicate_satisfied) {
    LeafNode* insert_node;
    std::stack<Node*> node_traceback;

    // If tree is empty
    if (root == nullptr) {
      root = new LeafNode();
      insert_node = &root;
    } else {
      insert_node = FindLeafNode(key, value, node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->SatisfiesPredicate(predicate)) {
      *predicate_satisfied = true;
      return false;  // The traverse function aborts the insert as key, val is present
    } else {
      *predicate_satisfied = false;
    }

    InsertAndPropagate(key, value, insert_node, node_traceback);

    return true;
  }
};
}  // namespace terrier::storage::index