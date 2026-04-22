#include <algorithm>
#include <cstdio>
#include <random>
#include <vector>
#include <set>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// Helper: verify all expected keys are present via GetValue
static void VerifyTreeContents(
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>>& tree,
    const std::set<int64_t>& expected) {
  GenericKey<8> k;
  std::vector<RID> rids;
  for (auto key : expected) {
    rids.clear();
    k.SetFromInteger(key);
    bool present = tree.GetValue(k, &rids);
    ASSERT_TRUE(present) << "Key " << key << " should be present";
    ASSERT_EQ(rids.size(), 1U) << "Key " << key;
    ASSERT_EQ(rids[0].GetSlotNum(), key) << "Key " << key;
  }
}

// Helper: verify iterator traverses all expected keys in order
static void VerifyIterator(
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>>& tree,
    const std::set<int64_t>& expected) {
  std::vector<int64_t> got;
  for (auto it = tree.Begin(); it != tree.End(); ++it) {
    got.push_back((*it).second.GetSlotNum());
  }
  std::vector<int64_t> exp(expected.begin(), expected.end());
  ASSERT_EQ(got.size(), exp.size());
  for (size_t i = 0; i < got.size(); i++) {
    ASSERT_EQ(got[i], exp[i]) << "at position " << i;
  }
}

TEST(BPlusTreeStrongTests, DeleteWithSplitsSequential) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id;
  bpm->NewPage(&page_id);
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 2, 3);  // small sizes!
  GenericKey<8> k;
  RID rid;
  auto* txn = new Transaction(0);

  std::set<int64_t> expected;
  // Insert 1..20 sequentially
  for (int64_t key = 1; key <= 20; key++) {
    rid.Set(0, key);
    k.SetFromInteger(key);
    tree.Insert(k, rid, txn);
    expected.insert(key);
  }
  VerifyTreeContents(tree, expected);
  VerifyIterator(tree, expected);

  // Delete 1..10 sequentially
  for (int64_t key = 1; key <= 10; key++) {
    k.SetFromInteger(key);
    tree.Remove(k, txn);
    expected.erase(key);
    VerifyTreeContents(tree, expected);
    VerifyIterator(tree, expected);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete txn;
  delete bpm;
}

TEST(BPlusTreeStrongTests, DeleteWithSplitsReverse) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id;
  bpm->NewPage(&page_id);
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> k;
  RID rid;
  auto* txn = new Transaction(0);

  std::set<int64_t> expected;
  for (int64_t key = 1; key <= 20; key++) {
    rid.Set(0, key);
    k.SetFromInteger(key);
    tree.Insert(k, rid, txn);
    expected.insert(key);
  }

  // Delete from 20 down to 11
  for (int64_t key = 20; key >= 11; key--) {
    k.SetFromInteger(key);
    tree.Remove(k, txn);
    expected.erase(key);
    VerifyTreeContents(tree, expected);
    VerifyIterator(tree, expected);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete txn;
  delete bpm;
}

TEST(BPlusTreeStrongTests, DeleteAll) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id;
  bpm->NewPage(&page_id);
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> k;
  RID rid;
  auto* txn = new Transaction(0);

  std::set<int64_t> expected;
  for (int64_t key = 1; key <= 15; key++) {
    rid.Set(0, key);
    k.SetFromInteger(key);
    tree.Insert(k, rid, txn);
    expected.insert(key);
  }

  // Delete all
  for (int64_t key = 1; key <= 15; key++) {
    k.SetFromInteger(key);
    tree.Remove(k, txn);
    expected.erase(key);
    VerifyTreeContents(tree, expected);
  }
  ASSERT_TRUE(tree.IsEmpty());

  // Reinsert and verify
  for (int64_t key = 1; key <= 15; key++) {
    rid.Set(0, key);
    k.SetFromInteger(key);
    tree.Insert(k, rid, txn);
    expected.insert(key);
  }
  VerifyTreeContents(tree, expected);
  VerifyIterator(tree, expected);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete txn;
  delete bpm;
}

TEST(BPlusTreeStrongTests, DeleteRandom) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(100, disk_manager.get());
  page_id_t page_id;
  bpm->NewPage(&page_id);
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 3, 4);
  GenericKey<8> k;
  RID rid;
  auto* txn = new Transaction(0);

  std::vector<int64_t> keys;
  for (int64_t key = 1; key <= 50; key++) keys.push_back(key);
  std::mt19937 rng(42);
  std::shuffle(keys.begin(), keys.end(), rng);

  std::set<int64_t> expected;
  for (auto key : keys) {
    rid.Set(0, key);
    k.SetFromInteger(key);
    tree.Insert(k, rid, txn);
    expected.insert(key);
  }
  VerifyTreeContents(tree, expected);
  VerifyIterator(tree, expected);

  std::shuffle(keys.begin(), keys.end(), rng);
  // Delete half
  for (size_t i = 0; i < keys.size() / 2; i++) {
    k.SetFromInteger(keys[i]);
    tree.Remove(k, txn);
    expected.erase(keys[i]);
    VerifyTreeContents(tree, expected);
    VerifyIterator(tree, expected);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete txn;
  delete bpm;
}

}  // namespace bustub
