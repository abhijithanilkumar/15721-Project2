#include "storage/index/bplustree.h"
#include "gtest/gtest.h"
#include "test_util/multithread_test_util.h"
#include "test_util/test_harness.h"

namespace terrier::storage::index {

struct BPlusTreeTests : public TerrierTest {
  const uint32_t num_threads_ =
      MultiThreadTestUtil::HardwareConcurrency() + (MultiThreadTestUtil::HardwareConcurrency() % 2);
};

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, BasicInsertAndRootSplit) {
  const uint32_t key_num = 15;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  EXPECT_EQ(tree->GetRoot(), nullptr);

  // Inserts the keys for the first time
  for (int i = 0; i < 5; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  EXPECT_TRUE(tree->GetRoot()->IsLeaf());

  // Inserts the keys for the second time with different value
  for (int i = 0; i < 5; i++) {
    tree->Insert(keys[i], keys[i] + 1);
  }

  // The root should not split as there are only 5 keys
  EXPECT_TRUE(tree->GetRoot()->IsLeaf());

  // Insert the other keys
  for (int i = 5; i < 15; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  // The root must split and the new root must be an inner node
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  int num_keys_one_val = 10;
  int num_keys_two_val = 5;

  size_t leaf_nodes_usage = sizeof(int64_t) * (num_keys_one_val * 2 + num_keys_two_val * 3);
  // Prev ptr is on stack, key, ptr pair is on the heap
  size_t root_node_usage = sizeof(int64_t) + sizeof(void *);

  EXPECT_EQ(tree->GetHeapUsage(), leaf_nodes_usage + root_node_usage);

  delete tree;
}

}  // namespace terrier::storage::index
