#include "storage/index/bplustree.h"
#include "gtest/gtest.h"
#include "test_util/multithread_test_util.h"
#include "test_util/test_harness.h"

namespace terrier::storage::index {
class BPlusTreeTests : public TerrierTest {
 public:
  const uint32_t num_threads_ = 4;
  common::WorkerPool thread_pool_{num_threads_, {}};

 protected:
  void SetUp() override { thread_pool_.Startup(); }

  void TearDown() override { thread_pool_.Shutdown(); }
};

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, SimpleScanKeyTest) {
  auto *const tree = new BPlusTree<int64_t, int64_t>;

  EXPECT_EQ(tree->GetRoot()->GetSize(), 0);

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
  const int key_num = FAN_OUT - 1;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  //  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  EXPECT_EQ(tree->GetRoot()->GetSize(), 0);

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
  const int key_num = FAN_OUT - 1;

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
TEST_F(BPlusTreeTests, MultiThreadedInsertTest) {
  const int key_num = FAN_OUT * FAN_OUT * FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;
  std::vector<int64_t> keys;
  keys.reserve(key_num);
  int64_t work_per_thread = key_num / num_threads_;
  for (int64_t i = 0; i < key_num; ++i) {
    keys.emplace_back(i);
  }
  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  auto workload = [&](uint32_t worker_id) {
    int64_t start = work_per_thread * worker_id;
    int64_t end = work_per_thread * (worker_id + 1);

    // Inserts the keys
    for (int i = start; i < end; i++) {
      tree->Insert(keys[i], keys[i]);
    }
  };

  // run the workload
  for (uint32_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();

  // Ensure all values are present
  for (int i = 0; i < key_num; i++) {
    std::vector<int64_t> results;
    tree->GetValue(keys[i], &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], keys[i]);
  }

  // The root must have split
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());
  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, RootSplitTest) {
  const int key_num = FAN_OUT;

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
  size_t space_for_values = key_num * sizeof(int64_t) + sizeof(void *);

  EXPECT_GE(tree->GetHeapUsage(), space_for_keys + space_for_values);

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, UniqueKeyValueInsert) {
  const int key_num = FAN_OUT - 1;

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
  const int key_num = FAN_OUT - 1;

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
  const int key_num = FAN_OUT * FAN_OUT;

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

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, SimpleDelete) {
  const int key_num = FAN_OUT - 1;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  EXPECT_EQ(tree->GetRoot()->GetSize(), 0);

  // Inserts the keys
  for (int i = 0; i < key_num; i++) {
    EXPECT_TRUE(tree->Insert(keys[i], keys[i]));
  }

  // The root node should not have split
  EXPECT_TRUE(tree->GetRoot()->IsLeaf());

  for (int i = 0; i < key_num; i++) {
    EXPECT_EQ(tree->GetRoot()->GetSize(), key_num - i);
    EXPECT_TRUE(tree->Delete(keys[i], keys[i]));
  }

  EXPECT_EQ(tree->GetRoot()->GetSize(), 0);

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, MultiValueDelete) {
  const int key_num = FAN_OUT - 1;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  EXPECT_EQ(tree->GetRoot()->GetSize(), 0);

  // Inserts the keys
  for (int i = 0; i < key_num; i++) {
    EXPECT_TRUE(tree->Insert(keys[i], keys[i]));
    EXPECT_TRUE(tree->Insert(keys[i], keys[i] + 1));
  }

  // The root node should not have split
  EXPECT_TRUE(tree->GetRoot()->IsLeaf());

  for (int i = 0; i < key_num; i++) {
    EXPECT_EQ(tree->GetRoot()->GetSize(), key_num - i);
    EXPECT_TRUE(tree->Delete(keys[i], keys[i]));
    EXPECT_EQ(tree->GetRoot()->GetSize(), key_num - i);
    EXPECT_TRUE(tree->Delete(keys[i], keys[i] + 1));
  }

  EXPECT_EQ(tree->GetRoot()->GetSize(), 0);

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, CoalesceLeavesOnDelete) {
  const int key_num = FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  // Inserts the keys
  for (int i = 0; i < key_num; i++) {
    EXPECT_TRUE(tree->Insert(i, i));
  }

  // The root node should have split
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_TRUE(tree->Delete(0, 0));

  EXPECT_TRUE(tree->GetRoot()->IsLeaf());
  EXPECT_EQ(tree->GetRoot()->GetSize(), FAN_OUT - 1);

  tree->Insert(0, 0);

  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_TRUE(tree->Delete(FAN_OUT - 1, FAN_OUT - 1));

  EXPECT_TRUE(tree->GetRoot()->IsLeaf());
  EXPECT_EQ(tree->GetRoot()->GetSize(), FAN_OUT - 1);

  // Ensure all values are present
  for (int i = 0; i < key_num - 1; i++) {
    std::vector<int64_t> results;
    tree->GetValue(i, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], i);
  }

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, BorrowFromLeafOnDelete) {
  const int key_num = FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  // Inserts the keys
  for (int i = 0; i < key_num; i++) {
    EXPECT_TRUE(tree->Insert(i, i));
  }

  // The root node should have split
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_TRUE(tree->Insert(FAN_OUT, FAN_OUT));

  EXPECT_TRUE(tree->Delete(0, 0));

  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_EQ(tree->GetRoot()->GetPrevPtr()->GetSize(), MIN_KEYS_LEAF_NODE);

  // Ensure all values are present
  for (int i = 1; i < key_num + 1; i++) {
    std::vector<int64_t> results;
    tree->GetValue(i, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], i);
  }

  std::vector<int64_t> results;
  tree->GetValue(0, &results);

  EXPECT_EQ(results.size(), 0);

  EXPECT_EQ(tree->GetHeightOfTree(), 2);

  // Borrow from left
  EXPECT_TRUE(tree->Insert(0, 0));

  EXPECT_TRUE(tree->Delete(FAN_OUT, FAN_OUT));

  for (int i = 0; i < key_num; i++) {
    std::vector<int64_t> results1;
    tree->GetValue(i, &results1);
    EXPECT_EQ(results1.size(), 1);
    EXPECT_EQ(results1[0], i);
  }

  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_EQ(tree->GetHeightOfTree(), 2);

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, BorrowFromInner) {
  const int key_num = 55;
  std::vector<int64_t> results;
  auto *const tree = new BPlusTree<int64_t, int64_t>;

  // Inserts the keys
  for (int i = 0; i < key_num; i++) {
    EXPECT_TRUE(tree->Insert(i, i));
  }

  // The root node should have split
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_EQ(tree->GetHeightOfTree(), 3);
  EXPECT_EQ(tree->GetRoot()->GetSize(), 1);

  // Test borrow right
  EXPECT_TRUE(tree->Delete(0, 0));

  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  // Ensure all values are present
  for (int i = 1; i < 50; i++) {
    results.clear();
    tree->GetValue(i, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], i);
  }

  results.clear();
  tree->GetValue(0, &results);

  EXPECT_EQ(results.size(), 0);

  EXPECT_EQ(tree->GetHeightOfTree(), 3);

  // Test borrow left

  // Cause overflow in leaf and create one more entry in left inner node
  EXPECT_TRUE(tree->Insert(0, 0));

  // Cause right inner node to underflow
  EXPECT_TRUE(tree->Delete(50, 50));

  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  // Ensure all values are present
  for (int i = 0; i < 55; i++) {
    if (i == 50) continue;
    results.clear();
    tree->GetValue(i, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], i);
  }

  results.clear();
  tree->GetValue(50, &results);

  EXPECT_EQ(results.size(), 0);

  EXPECT_EQ(tree->GetHeightOfTree(), 3);

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, CoalesceToRightInner) {
  const int key_num = 55;
  std::vector<int64_t> results;
  auto *const tree = new BPlusTree<int64_t, int64_t>;

  // Inserts the keys
  for (int i = 0; i < key_num; i++) {
    EXPECT_TRUE(tree->Insert(i, i));
  }

  // The root node should have split
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_EQ(tree->GetHeightOfTree(), 3);
  EXPECT_EQ(tree->GetRoot()->GetSize(), 1);

  // Test Coalesce to right
  EXPECT_TRUE(tree->Delete(50, 50));
  EXPECT_TRUE(tree->Delete(0, 0));

  EXPECT_EQ(tree->GetHeightOfTree(), 2);
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  // Ensure all values are present
  for (int i = 1; i < 55; i++) {
    if (i == 50) continue;
    results.clear();
    tree->GetValue(i, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], i);
  }

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, CoalesceToLeftInner) {
  const int key_num = 55;
  std::vector<int64_t> results;
  auto *const tree = new BPlusTree<int64_t, int64_t>;

  // Inserts the keys
  for (int i = 0; i < key_num; i++) {
    EXPECT_TRUE(tree->Insert(i, i));
  }

  // The root node should have split
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  EXPECT_EQ(tree->GetHeightOfTree(), 3);
  EXPECT_EQ(tree->GetRoot()->GetSize(), 1);

  // Test Coalesce to left
  EXPECT_TRUE(tree->Delete(0, 0));
  EXPECT_TRUE(tree->Delete(50, 50));

  EXPECT_EQ(tree->GetHeightOfTree(), 2);
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  // Ensure all values are present
  for (int i = 1; i < 55; i++) {
    if (i == 50) continue;
    results.clear();
    tree->GetValue(i, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], i);
  }

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, RootInnerToLeaf) {
  const int key_num = FAN_OUT * FAN_OUT * FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  for (int i = 0; i < key_num; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  for (int i = 0; i < key_num - 1; i++) {
    tree->Delete(keys[i], keys[i]);
  }

  EXPECT_TRUE(tree->GetRoot()->IsLeaf());
  EXPECT_EQ(tree->GetRoot()->GetSize(), 1);

  tree->Delete(keys[key_num - 1], keys[key_num - 1]);

  EXPECT_EQ(tree->GetRoot()->GetSize(), 0);

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, MultiThreadedDeleteTest) {
  const int key_num = FAN_OUT * FAN_OUT * FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;
  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; ++i) {
    keys.emplace_back(i);
  }
  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT
  for (int i = 0; i < key_num; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  const int deleted_keys = key_num / 2;
  int64_t work_per_thread = deleted_keys / num_threads_;

  auto workload = [&](uint32_t worker_id) {
    int64_t start = work_per_thread * worker_id;
    int64_t end = work_per_thread * (worker_id + 1);

    // Inserts the keys
    for (int i = start; i < end; i++) {
      tree->Delete(keys[i], keys[i]);
    }
  };

  // run the workload
  for (uint32_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();

  // Ensure all values are present
  for (int i = deleted_keys; i < key_num; i++) {
    std::vector<int64_t> results;
    tree->GetValue(keys[i], &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], keys[i]);
  }

  // The root must have split
  EXPECT_FALSE(tree->GetRoot()->IsLeaf());

  delete tree;
}

TEST_F(BPlusTreeTests, ScanAscendingRootSorted) {
  const int key_num = FAN_OUT - 1;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  for (int i = 0; i < key_num; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  int i = 0;
  for (auto it = tree->Begin(); !(it == tree->End()); ++it, ++i) {
    EXPECT_EQ(it.first_, keys[i]);
    EXPECT_EQ(it.second_, keys[i]);
  }

  EXPECT_EQ(i, key_num);

  delete tree;
}

TEST_F(BPlusTreeTests, ScanAscendingRootShuffled) {
  const int key_num = FAN_OUT - 1;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  for (int i = 0; i < key_num; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  int i = 0;
  for (auto it = tree->Begin(); !(it == tree->End()); ++it, ++i) {
    EXPECT_EQ(it.first_, i);
    EXPECT_EQ(it.second_, i);
  }

  EXPECT_EQ(i, key_num);

  delete tree;
}

TEST_F(BPlusTreeTests, ScanAscendingInsertTwoLevelShuffled) {
  const int key_num = FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  for (int i = 0; i < key_num; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  int i = 0;
  for (auto it = tree->Begin(); !(it == tree->End()); ++it, ++i) {
    EXPECT_EQ(it.first_, i);
    EXPECT_EQ(it.second_, i);
  }

  EXPECT_EQ(i, key_num);

  delete tree;
}

TEST_F(BPlusTreeTests, ScanAscendingInsertMultiLevelShuffled) {
  const int key_num = FAN_OUT * FAN_OUT * FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  for (int i = 0; i < key_num; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  int i = 0;
  for (auto it = tree->Begin(); !(it == tree->End()); ++it, ++i) {
    EXPECT_EQ(it.first_, i);
    EXPECT_EQ(it.second_, i);
  }

  EXPECT_EQ(i, key_num);

  delete tree;
}

TEST_F(BPlusTreeTests, ScanAscendingDeleteTwoLevelShuffled) {
  const int key_num = FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  for (int i = 0; i < key_num; i++) {
    tree->Insert(keys[i], keys[i]);
  }

  for (int i = 0; i < (key_num + 1) / 2; i++) {
    EXPECT_TRUE(tree->Delete(keys[i], keys[i]));
  }

  int i = 0;
  for (auto it = tree->Begin(); !(it == tree->End()); ++it, ++i)
    ;

  EXPECT_EQ(i, key_num / 2);

  delete tree;
}

TEST_F(BPlusTreeTests, ScanAscendingDeleteMultiLevelShuffled) {
  const int key_num = FAN_OUT * FAN_OUT * FAN_OUT;

  auto *const tree = new BPlusTree<int64_t, int64_t>;
  EXPECT_TRUE(tree->CheckStructuralIntegrity());

  std::vector<std::pair<int64_t, int64_t> > keys;
  keys.reserve(3 * key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(std::make_pair(i, i + 1));
    keys.emplace_back(std::make_pair(i, i + 2));
    keys.emplace_back(std::make_pair(i, i + 3));
  }

  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  for (int i = 0; i < 3 * key_num; i++) {
    tree->Insert(keys[i].first, keys[i].second);
    EXPECT_TRUE(tree->CheckStructuralIntegrity());
  }

  for (int i = 0; i < 3 * key_num; i++) {
    EXPECT_TRUE(tree->Delete(keys[i].first, keys[i].second));

    std::vector<int64_t> results;

    tree->GetValue(keys[i].first, &results);

    for (auto it : results) {
      EXPECT_FALSE(it == keys[i].second);
    }

    int j = 0;
    for (auto it = tree->Begin(); !(it == tree->End()); ++it, ++j)
      ;

    EXPECT_EQ(j, 3 * key_num - i - 1);
    EXPECT_TRUE(tree->CheckStructuralIntegrity());
  }

  delete tree;
}

}  // namespace terrier::storage::index
