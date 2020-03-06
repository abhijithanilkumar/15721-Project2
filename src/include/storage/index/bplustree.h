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

   public:
    /*
 * Begin() - Returns a begin iterator to its internal array
 */
    inline typename std::vector<KeyValueListPair>::iterator begin() { return entries.begin(); }

    inline typename std::vector<KeyValueListPair>::iterator begin() const { return entries.begin(); }

    /*
     * End() - Returns an end iterator that is similar to the one for vector
     */
    inline typename std::vector<KeyValueListPair>::iterator end() { return entries.end(); }

    inline const typename std::vector<KeyValueListPair>::iterator end() const { return entries.end(); }

    /*
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    inline const typename std::vector<KeyValueListPair>::iterator rend() { return entries.rend(); }

    inline const typename std::vector<KeyValueListPair>::iterator rend() const { return entries.rend(); }

    /*
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    inline int size() const { return entries.size(); }

    inline void insert(const KeyType key, const ValueType value) {
      // TODO : Search for correct position and insert value
    }

    /*
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    inline void insert(const typename std::vector<KeyValueListPair>::iterator copy_start_p,
                       const typename std::vector<KeyValueListPair>::iterator copy_end_p) {
      // Make sure the loop will come to an end
      TERRIER_ASSERT(copy_start_p <= copy_end_p, "Loop will not come to an end.");

      entries.insert(entries.end(), copy_start_p, copy_end_p);
    }
  };

  class InnerNode: public Node {
   private:
    std::vector<KeyNodePtrPair> entries;

   public:
    /*
 * Begin() - Returns a begin iterator to its internal array
 */
    inline typename std::vector<KeyNodePtrPair>::iterator begin() { return entries.begin(); }

    inline const typename std::vector<KeyNodePtrPair>::iterator begin() const { return entries.begin(); }

    /*
     * End() - Returns an end iterator that is similar to the one for vector
     */
    inline typename std::vector<KeyNodePtrPair>::iterator end() { return entries.end(); }

    inline const typename std::vector<KeyNodePtrPair>::iterator end() const { return entries.end(); }

    /*
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    inline const typename std::vector<KeyNodePtrPair>::iterator rend() { return entries.rend(); }

    inline const typename std::vector<KeyNodePtrPair>::iterator rend() const { return entries.rend(); }

    /*
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    inline int size() const { return entries.size(); }

    inline void insert(const KeyNodePtrPair &element) { entries.insert(element); }

    /*
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    inline void insert(const typename std::vector<KeyNodePtrPair>::iterator copy_start_p,
                       const typename std::vector<KeyNodePtrPair>::iterator copy_end_p) {
      // Make sure the loop will come to an end
      TERRIER_ASSERT(copy_start_p <= copy_end_p, "Loop will not come to an end.");
      entries.insert(entries.end(), copy_start_p, copy_end_p);
    }
  };

  // Root of the tree
  Node* root;

 public:
  void InsertAndPropagate(const KeyType &key, const ValueType &value, LeafNode* insert_node,
      std::stack<Node*>& node_traceback) {
    insert_node->insert(key, value);

    // If insertion causes overflow
    if (insert_node->isOverflow()) {
      // create new leaf node and insert key, value
      LeafNode* new_node = new LeafNode();

      // Move half of entries to the new node and update sibling pointers
      insert_node->split(new_node);

      Node* child_node = new_node;

      while (!node_traceback.empty()) {
        InnerNode* parent_node = dynamic_cast<InnerNode*> (node_traceback.top());
        node_traceback.pop();

        // Insert an entry for the child node in the parent node
        parent_node->insert(child_node->getFirstKey(), child_node);

        // If no overflow done with inserting
        if (!parent_node->isOverflow()) {
          break;
        }

        // Create a new node and split
        new_node = new InnerNode();
        parent_node->split(new_node);

        // If split node is a root, we need to add a new root and link the splits
        if (parent_node == root) {
          root = new InnerNode();
          root->insert(parent_node->getFirstKey(), parent_node);
          root->insert(new_node->getFirstKey(), new_node);
          break;
        }

        // Set as child node for next iteration
        child_node = new_node;
      }
    }
  }

  bool Insert(const KeyType &key, const ValueType &value, bool unique_key) {
    std::stack<Node*> node_traceback;
    LeafNode* insert_node;

    // If tree is empty
    if (root == nullptr) {
      root = new LeafNode();
      insert_node = &root;
    } else {
      insert_node = findLeafNode(key, value, node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->hasKeyVal(key, value) || (unique_key && insert_node->hasKey(key))) {
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
      insert_node = findLeafNode(key, value, node_traceback);  // Node traceback passed as ref
    }

    // If there were conflicting key values
    if (insert_node->satisfiesPredicate(predicate)) {
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