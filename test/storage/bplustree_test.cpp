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
  TEST_F(BPlusTreeTests, SimpleScanKeyTest) {
    auto *const tree = new BPlusTree<int64_t, int64_t>;

    EXPECT_EQ(tree->GetRoot(), nullptr);

    // Inserts the keys
    EXPECT_TRUE(tree->Insert(0, 10));

    // Ensure all values are present
    std::vector<int64_t> results;
    tree->GetValue(0, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 10);

    // Check the heap usage
    EXPECT_GE(tree->GetHeapUsage(), 2 * sizeof(int64_t));

    delete tree;
  }

  // NOLINTNEXTLINE
  TEST_F(BPlusTreeTests, MultipleKeyInsert) {
    const uint32_t key_num = FAN_OUT - 1;

    auto *const tree = new BPlusTree<int64_t, int64_t>;

    std::vector<int64_t> keys;
    keys.reserve(key_num);

    for (int64_t i = 0; i < key_num; i++) {
      keys.emplace_back(i);
    }

    std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

    EXPECT_EQ(tree->GetRoot(), nullptr);

    // Inserts the keys
    for (int i = 0; i < key_num; i++) {
      EXPECT_TRUE(tree->Insert(keys[i], keys[i]));
    }

    // Ensure all values are present
    for (int i = 0; i < key_num; i++) {
      std::vector<int64_t> results;
      tree->GetValue(keys[i], &results);
      EXPECT_EQ(results.size(), 1);
      EXPECT_EQ(results[0], keys[i]);
    }

    // The root node should not have split
    EXPECT_TRUE(tree->GetRoot()->IsLeaf());

    size_t space_for_keys = key_num * sizeof(int64_t);
    size_t space_for_values = key_num * sizeof(int64_t);

    // Check the heap usage
    EXPECT_GE(tree->GetHeapUsage(), space_for_keys + space_for_values);

    delete tree;
  }

  // NOLINTNEXTLINE
  TEST_F(BPlusTreeTests, DuplicateInsert) {
    const uint32_t key_num = FAN_OUT - 1;

    auto *const tree = new BPlusTree<int64_t, int64_t>;

    std::vector<int64_t> keys;
    keys.reserve(key_num);

    for (int64_t i = 0; i < key_num; i++) {
      keys.emplace_back(i);
    }

    std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

    // Inserts the keys with 2 different values
    for (int i = 0; i < key_num; i++) {
      EXPECT_TRUE(tree->Insert(keys[i], keys[i]));
      EXPECT_TRUE(tree->Insert(keys[i], keys[i] + 1));
    }

    // There are 2 * (FAN_OUT - 1) values but only FAN_OUT-1 keys, so root must not split
    EXPECT_TRUE(tree->GetRoot()->IsLeaf());

    // Ensure all values are present
    for (int i = 0; i < key_num; i++) {
      std::vector<int64_t> results;
      tree->GetValue(keys[i], &results);
      EXPECT_EQ(results.size(), 2);
      std::sort(results.begin(), results.end());
      EXPECT_EQ(results[0], keys[i]);
      EXPECT_EQ(results[1], keys[i] + 1);
    }

    size_t space_for_keys = key_num * sizeof(int64_t);
    size_t space_for_values = 2 * key_num * sizeof(int64_t);

    EXPECT_GE(tree->GetHeapUsage(), space_for_keys + space_for_values);

    delete tree;
  }

  // NOLINTNEXTLINE
  TEST_F(BPlusTreeTests, RootSplitTest) {
    const uint32_t key_num = FAN_OUT;

    auto *const tree = new BPlusTree<int64_t, int64_t>;

    std::vector<int64_t> keys;
    keys.reserve(key_num);

    for (int64_t i = 0; i < key_num; i++) {
      keys.emplace_back(i);
    }

    std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

    // Inserts the keys
    for (int i = 0; i < key_num; i++) {
      tree->Insert(keys[i], keys[i]);
    }

    // The root must have split
    EXPECT_FALSE(tree->GetRoot()->IsLeaf());

    // Ensure all values are present
    for (int i = 0; i < key_num; i++) {
      std::vector<int64_t> results;
      tree->GetValue(keys[i], &results);
      EXPECT_EQ(results.size(), 1);
      EXPECT_EQ(results[0], keys[i]);
    }

    size_t space_for_keys = (key_num + 1) * sizeof(int64_t);
    // Prev ptr is on the stack, the next ptr of the middle key on the root is in the heap
    size_t space_for_values = key_num * sizeof(int64_t) + sizeof(void*);

    EXPECT_GE(tree->GetHeapUsage(), space_for_keys + space_for_values);

    delete tree;
  }

  // NOLINTNEXTLINE
  TEST_F(BPlusTreeTests, UniqueKeyValueInsert) {
    const uint32_t key_num = FAN_OUT - 1;

    auto *const tree = new BPlusTree<int64_t, int64_t>;

    std::vector<int64_t> keys;
    keys.reserve(key_num);

    for (int64_t i = 0; i < key_num; i++) {
      keys.emplace_back(i);
    }

    std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

    // Inserts the keys
    for (int i = 0; i < key_num; i++) {
      EXPECT_TRUE(tree->Insert(keys[i], keys[i]));
    }

    // Store the heap usage
    size_t heap_usage = tree->GetHeapUsage();

    // Inserts the keys with the same value
    for (int i = 0; i < key_num; i++) {
      // The insert must fail
      EXPECT_FALSE(tree->Insert(keys[i], keys[i]));
    }


    // Ensure all values are present and no duplicates
    for (int i = 0; i < key_num; i++) {
      std::vector<int64_t> results;
      tree->GetValue(keys[i], &results);
      EXPECT_EQ(results.size(), 1);
      EXPECT_EQ(results[0], keys[i]);
    }

    // The heap usage must not have changed
    EXPECT_EQ(tree->GetHeapUsage(), heap_usage);

    delete tree;
  }

  // NOLINTNEXTLINE
  TEST_F(BPlusTreeTests, UniqueKeyInsert) {
    const uint32_t key_num = FAN_OUT - 1;

    auto *const tree = new BPlusTree<int64_t, int64_t>;

    std::vector<int64_t> keys;
    keys.reserve(key_num);

    for (int64_t i = 0; i < key_num; i++) {
      keys.emplace_back(i);
    }

    std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

    // Inserts the keys
    for (int i = 0; i < key_num; i++) {
      EXPECT_TRUE(tree->Insert(keys[i], keys[i], true));
    }

    // Store the heap usage
    size_t heap_usage = tree->GetHeapUsage();

    // Inserts the keys with a different value
    for (int i = 0; i < key_num; i++) {
      // The insert must fail
      EXPECT_FALSE(tree->Insert(keys[i], keys[i] + 1, true));
    }

    // Ensure all values are present and are correct
    for (int i = 0; i < key_num; i++) {
      std::vector<int64_t> results;
      tree->GetValue(keys[i], &results);
      EXPECT_EQ(results.size(), 1);
      EXPECT_EQ(results[0], keys[i]);
    }

    // The heap usage must not have changed
    EXPECT_EQ(tree->GetHeapUsage(), heap_usage);

    delete tree;
  }

  // NOLINTNEXTLINE
  TEST_F(BPlusTreeTests, InnerNodeSplit) {
    const uint32_t key_num = FAN_OUT * FAN_OUT;

    auto *const tree = new BPlusTree<int64_t, int64_t>;

    // Inserts the keys
    for (int i = 0; i < key_num; i++) {
      EXPECT_TRUE(tree->Insert(i, i));
    }

    // Since the max number of keys that can fit in level 2 is FAN_OUT * (FAN_OUT - 1), must ensure height is >= 3
    EXPECT_GE(tree->GetHeightOfTree(), 3);

    // Ensure all values are present
    for (int i = 0; i < key_num; i++) {
      std::vector<int64_t> results;
      tree->GetValue(i, &results);
      EXPECT_EQ(results.size(), 1);
      EXPECT_EQ(results[0], i);
    }

    delete tree;
  }
}  // namespace terrier::storage::index
