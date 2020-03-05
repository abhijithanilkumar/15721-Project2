#pragma once

#include <functional>
#include "common/macros.h"


namespace terrier::storage::index {


template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
    typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
    typename ValueEqualityChecker = std::equal_to<ValueType>>
class BPlusTree {
  class KeyNodeIDPairComparator;
  class KeyNodeIDPairHashFunc;
  class KeyNodeIDPairEqualityChecker;
  class KeyValuePairHashFunc;
  class KeyValuePairEqualityChecker;

  using KeyNodeIDPair = std::pair<KeyType, int>;

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
    // For all nodes including base node and data node and SMO nodes,
    // the low key pointer always points to a KeyNodeIDPair structure
    // inside the base node, which is either the first element of the
    // node sep list (InnerNode), or a class member (LeafNode)
    const KeyNodeIDPair *low_key_p;

    // high key points to the KeyNodeIDPair inside the LeafNode and InnerNode
    // if there is neither SplitNode nor MergeNode. Otherwise it
    // points to the item inside split node or merge right sibling branch
    const KeyNodeIDPair *high_key_p;

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
    NodeMetaData(const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p, NodeType p_type, int p_depth,
                 int p_item_count)
        : low_key_p{p_low_key_p},
          high_key_p{p_high_key_p},
          type{p_type},
          depth{static_cast<short>(p_depth)},
          item_count{p_item_count} {}
  };

  /*
  * class BaseNode - Generic node class; inherited by leaf, inner
  *                  and delta node
  */
  class BaseNode {
    // We hold its data structure as private to force using member functions
    // for member access
   private:

    // This holds low key, high key, next node ID, type, depth and item count
    NodeMetaData metadata;

   public:
    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType p_type, const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p, int p_depth,
             int p_item_count)
        : metadata{p_low_key_p, p_high_key_p, p_type, p_depth, p_item_count} {}

    /*
     * Type() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const { return metadata.type; }

    /*
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    inline const NodeMetaData &GetNodeMetaData() const { return metadata; }

    /*
     * IsDeltaNode() - Return whether a node is delta node
     *
     * All nodes that are neither inner nor leaf type are of
     * delta node type
     */
    inline bool IsDeltaNode() const { return !(GetType() == NodeType::InnerType || GetType() == NodeType::LeafType); }

    /*
     * IsInnerNode() - Returns true if the node is an inner node
     *
     * This is useful if we want to collect all seps on an inner node
     * If the top of delta chain is an inner node then just do not collect
     * and use the node directly
     */
    inline bool IsInnerNode() const { return GetType() == NodeType::InnerType; }

    /*
     * IsRemoveNode() - Returns true if the node is of inner/leaf remove type
     *
     * This is used in JumpToLeftSibling() as an assertion
     */
    inline bool IsRemoveNode() const {
      return (GetType() == NodeType::InnerRemoveType) || (GetType() == NodeType::LeafRemoveType);
    }

    /*
     * IsOnLeafDeltaChain() - Return whether the node is part of
     *                        leaf delta chain
     *
     * This is true even for NodeType::LeafType
     *
     * NOTE: WHEN ADDING NEW NODE TYPES PLEASE UPDATE THIS LIST
     *
     * Note 2: Avoid calling this in multiple places. Currently we only
     * call this in TakeNodeSnapshot() or in the debugger
     *
     * This function makes use of the fact that leaf types occupy a
     * continuous region of NodeType numerical space, so that we could
     * the identity of leaf or Inner using only one comparison
     *
     */
    inline bool IsOnLeafDeltaChain() const { return GetType() >= NodeType::LeafStart; }

    /*
     * GetLowKey() - Returns the low key of the current base node
     *
     * NOTE: Since it is defined that for LeafNode the low key is undefined
     * and pointers should be set to nullptr, accessing the low key of
     * a leaf node would result in Segmentation Fault
     */
    inline const KeyType &GetLowKey() const { return metadata.low_key_p->first; }

    /*
     * GetHighKey() - Returns a reference to the high key of current node
     *
     * This function could be called for all node types including leaf nodes
     * and inner nodes.
     */
    inline const KeyType &GetHighKey() const { return metadata.high_key_p->first; }

    /*
     * GetHighKeyPair() - Returns the pointer to high key node id pair
     */
    inline const KeyNodeIDPair &GetHighKeyPair() const { return *metadata.high_key_p; }

    /*
     * GetLowKeyPair() - Returns the pointer to low key node id pair
     *
     * The return value is nullptr for LeafNode and its delta chain
     */
    inline const KeyNodeIDPair &GetLowKeyPair() const { return *metadata.low_key_p; }

    /*
     * GetNextNodeID() - Returns the next NodeID of the current node
     */
    inline int GetNextNodeID() const { return metadata.high_key_p->second; }

    /*
     * GetLowKeyNodeID() - Returns the NodeID for low key
     *
     * NOTE: This function should not be called for leaf nodes
     * since the low key node ID for leaf node is not defined
     */
    inline int GetLowKeyNodeID() const {
      TERRIER_ASSERT(!IsOnLeafDeltaChain(), "This should not be called on leaf nodes.");

      return metadata.low_key_p->second;
    }

    /*
     * GetDepth() - Returns the depth of the current node
     */
    inline int GetDepth() const { return metadata.depth; }

    /*
     * GetItemCount() - Returns the item count of the current node
     */
    inline int GetItemCount() const { return metadata.item_count; }

    /*
     * SetLowKeyPair() - Sets the low key pair of metadata
     */
    inline void SetLowKeyPair(const KeyNodeIDPair *p_low_key_p) { metadata.low_key_p = p_low_key_p; }

    /*
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    inline void SetHighKeyPair(const KeyNodeIDPair *p_high_key_p) { metadata.high_key_p = p_high_key_p; }
  };

  /*
   * class ElasticNode - The base class for elastic node types, i.e. InnerNode
   *                     and LeafNode
   *
   * Since for InnerNode and LeafNode, the number of elements is not a compile
   * time known constant. However, for efficient tree traversal we must inline
   * all elements to reduce cache misses with workload that's less predictable
   */
  template <typename ElementType>
  class ElasticNode : public BaseNode {
   private:
    // These two are the low key and high key of the node respectively
    // since we could not add it in the inherited class (will clash with
    // the array which is invisible to the compiler) so they must be added here
    KeyNodeIDPair low_key;
    KeyNodeIDPair high_key;

    // This is the end of the elastic array
    // We explicitly store it here to avoid calculating the end of the array
    // everytime
    ElementType *end;

    // This is the starting point
    ElementType start[0];

   public:
    /*
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */
    ElasticNode(NodeType p_type, int p_depth, int p_item_count, const KeyNodeIDPair &p_low_key,
                const KeyNodeIDPair &p_high_key)
        : BaseNode{p_type, &low_key, &high_key, p_depth, p_item_count},
          low_key{p_low_key},
          high_key{p_high_key},
          end{start} {}

    /*
     * Copy() - Copy constructs another instance
     */
    static ElasticNode *Copy(const ElasticNode &other) {
      ElasticNode *node_p = ElasticNode::Get(other.GetItemCount(), other.GetType(), other.GetDepth(),
                                             other.GetItemCount(), other.GetLowKeyPair(), other.GetHighKeyPair());

      node_p->PushBack(other.Begin(), other.End());

      return node_p;
    }

    /*
     * Destructor
     *
     * All element types are destroyed inside the destruction function. D'tor
     * is called by Destroy(), and therefore should not be called directly
     * by external functions.
     *
     * Note that this is not called by Destroy() and instead it should be
     * called by an external function that destroies a delta chain, since in one
     * instance of thie class there might be multiple nodes of different types
     * so destroying should be dont individually with each type.
     */
    ~ElasticNode() {
      // Use two iterators to iterate through all existing elements
      for (ElementType *element_p = Begin(); element_p != End(); element_p++) {
        // Manually calls destructor when the node is destroyed
        element_p->~ElementType();
      }
    }

    /*
     * Destroy() - Frees the memory by calling AllocationMeta::Destroy()
     *
     * Note that function does not call destructor, and instead the destructor
     * should be called first before this function is called
     *
     * Note that this function does not call destructor for the class since
     * it holds multiple instances of tree nodes, we should call destructor
     * for each individual type outside of this class, and only frees memory
     * when Destroy() is called.
     */
    void Destroy() const {
      // This finds the allocation header for this base node, and then
      // traverses the linked list
      ElasticNode::GetAllocationHeader(this)->Destroy();
    }

    /*
     * Begin() - Returns a begin iterator to its internal array
     */
    inline ElementType *Begin() { return start; }

    inline const ElementType *Begin() const { return start; }

    /*
     * End() - Returns an end iterator that is similar to the one for vector
     */
    inline ElementType *End() { return end; }

    inline const ElementType *End() const { return end; }

    /*
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    inline const ElementType *REnd() { return start - 1; }

    inline const ElementType *REnd() const { return start - 1; }

    /*
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    inline int GetSize() const { return static_cast<int>(End() - Begin()); }

    /*
     * PushBack() - Push back an element
     *
     * This function takes an element type and copy-construct it on the array
     * which is invisible to the compiler. Therefore we must call placement
     * operator new to do the job
     */
    inline void PushBack(const ElementType &element) {
      // Placement new + copy constructor using end pointer
      new (end) ElementType{element};

      // Move it pointing to the enxt available slot, if not reached the end
      end++;
    }

    /*
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    inline void PushBack(const ElementType *copy_start_p, const ElementType *copy_end_p) {
      // Make sure the loop will come to an end
      TERRIER_ASSERT(copy_start_p <= copy_end_p, "Loop will not come to an end.");

      while (copy_start_p != copy_end_p) {
        PushBack(*copy_start_p);
        copy_start_p++;
      }
    }

   public:
    /*
     * Get() - Static helper function that constructs a elastic node of
     *         a certain size
     *
     * Note that since operator new is only capable of allocating a fixed
     * sized structure, we need to call malloc() directly to deal with variable
     * lengthed node. However, after malloc() returns we use placement operator
     * new to initialize it, such that the node could be freed using operator
     * delete later on
     */
//    inline static ElasticNode *Get(int size,  // Number of elements
//                                   NodeType p_type, int p_depth,
//                                   int p_item_count,  // Usually equal to size
//                                   const KeyNodeIDPair &p_low_key, const KeyNodeIDPair &p_high_key) {
//      // Currently this is always true - if we want a larger array then
//      // just remove this line
//      TERRIER_ASSERT(size == p_item_count, "Remove this if you want a larger array.");
//
//      // Allocte memory for
//      //   1. AllocationMeta (chunk)
//      //   2. node meta
//      //   3. ElementType array
//      // basic template + ElementType element size * (node size) + CHUNK_SIZE()
//      // Note: do not make it constant since it is going to be modified
//      // after being returned
////      auto *alloc_base = new char[sizeof(ElasticNode) + size * sizeof(ElementType) + AllocationMeta::CHUNK_SIZE()];
////      TERRIER_ASSERT(alloc_base != nullptr, "Allocation failed.");
//
//      // Initialize the AllocationMeta - tail points to the first byte inside
//      // class ElasticNode; limit points to the first byte after class
//      // AllocationMeta
////      new (reinterpret_cast<AllocationMeta *>(alloc_base))
////          AllocationMeta{alloc_base + AllocationMeta::CHUNK_SIZE(), alloc_base + sizeof(AllocationMeta)};
//
//      // The first CHUNK_SIZE() byte is used by class AllocationMeta
//      // and chunk data
////      auto *node_p = reinterpret_cast<ElasticNode *>(alloc_base + AllocationMeta::CHUNK_SIZE());
//
//      // Call placement new to initialize all that could be initialized
//      new (node_p) ElasticNode{p_type, p_depth, p_item_count, p_low_key, p_high_key};
//
//      return node_p;
//    }

    /*
     * GetNodeHeader() - Given low key pointer, returns the node header
     *
     * This is useful since only the low key pointer is available from any
     * type of node
     */
    static ElasticNode *GetNodeHeader(const KeyNodeIDPair *low_key_p) {
      static constexpr size_t low_key_offset = offsetof(ElasticNode, low_key);

      return reinterpret_cast<ElasticNode *>(reinterpret_cast<uint64_t>(low_key_p) - low_key_offset);
    }

    /*
     * GetAllocationHeader() - Returns the address of class AllocationHeader
     *                         embedded inside the ElasticNode object
     */
//    static AllocationMeta *GetAllocationHeader(const ElasticNode *node_p) {
//      return reinterpret_cast<AllocationMeta *>(reinterpret_cast<uint64_t>(node_p) - AllocationMeta::CHUNK_SIZE());
//    }

//    /*
//     * InlineAllocate() - Allocates a delta node in preallocated area preceeds
//     *                    the data area of this ElasticNode
//     *
//     * Note that for any given NodeType, we always know its low key and the
//     * low key always points to the struct inside base node. This way, we
//     * compute the offset of the low key from the begining of the struct,
//     * and then subtract it with CHUNK_SIZE() to derive the address of
//     * class AllocationMeta
//     *
//     * Note that since this function is accessed when the header is unknown
//     * so (1) it is static, and (2) it takes low key p which is universally
//     * available for all node type (stored in NodeMetadata)
//     */
//    static void *InlineAllocate(const KeyNodeIDPair *low_key_p, size_t size) {
//      const ElasticNode *node_p = GetNodeHeader(low_key_p);
//      TERRIER_ASSERT(&node_p->low_key == low_key_p, "low_key is not low_key_p.");
//
//      // Jump over chunk content
//      AllocationMeta *meta_p = GetAllocationHeader(node_p);
//
//      void *p = meta_p->Allocate(size);
//      TERRIER_ASSERT(p != nullptr, "Allocation failed.");
//
//      return p;
//    }
  
    /*
     * At() - Access element with bounds checking under debug mode
     */
    inline ElementType &At(const int index) {
      // The index must be inside the valid range
      TERRIER_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }

    inline const ElementType &At(const int index) const {
      // The index must be inside the valid range
      TERRIER_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }
  };

};
}  // namespace terrier::storage::index