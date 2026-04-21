#include "storage/index/b_plus_tree.h"

#include <sstream>
#include <string>
#include <stack>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub
{

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id,
                          BufferPoolManager* buffer_pool_manager,
                          const KeyComparator& comparator, int leaf_max_size,
                          int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id)
{
  WritePageGuard guard = bpm_ -> FetchPageWrite(header_page_id_);
  // In the original bpt, I fetch the header page
  // thus there's at least one page now
  auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  // reinterprete the data of the page into "HeaderPage"
  root_header_page -> root_page_id_ = INVALID_PAGE_ID;
  // set the root_id to INVALID
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const  ->  bool
{
  ReadPageGuard guard = bpm_ -> FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  bool is_empty = root_header_page -> root_page_id_ == INVALID_PAGE_ID;
  // Just check if the root_page_id is INVALID
  // usage to fetch a page:
  // fetch the page guard   ->   call the "As" function of the page guard
  // to reinterprete the data of the page as "BPlusTreePage"
  return is_empty;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType& key,
                              std::vector<ValueType>* result, Transaction* txn)
     ->  bool
{
  page_id_t rootId = GetRootPageId();
  if (rootId == INVALID_PAGE_ID) {return false;}
  auto currentPageGuard = bpm_ -> FetchPageRead(rootId);
  while (!currentPageGuard.template As<BPlusTreePage>() -> IsLeafPage()) {
    const InternalPage* currentInternalPage = currentPageGuard.template As<InternalPage>();
    int index = BinaryFind(currentInternalPage, key);
    currentPageGuard = bpm_ -> FetchPageRead(currentInternalPage -> ValueAt(index));
  }
  const LeafPage* targetLeaf = currentPageGuard.template As<LeafPage>();
  int targetIndex = BinaryFind(targetLeaf, key);
  if (targetIndex != -1 && comparator_(targetLeaf -> KeyAt(targetIndex), key) == 0) {
    result -> push_back(targetLeaf -> ValueAt(targetIndex));
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType& key, const ValueType& value,
                            Transaction* txn)  ->  bool
{
  if (IsEmpty()){
    page_id_t id;
    auto newPageGuard = bpm_ -> NewPageGuarded(&id);
    auto headerPageGuard = bpm_ -> FetchPageWrite(header_page_id_);
    auto currentHead = headerPageGuard.template AsMut<BPlusTreeHeaderPage>();
    currentHead -> root_page_id_ = id;
    auto writePageGuard = newPageGuard.UpgradeWrite();
    LeafPage* newPage = writePageGuard.template AsMut<LeafPage>();
    newPage -> Init(leaf_max_size_);
    newPage -> IncreaseSize(1);
    newPage -> SetAt(0, key, value);
    return true; 
  }//插入第一个键值对的情况
  std::stack<std::pair<page_id_t, int>> parentId{};//父亲页和插入位置
  auto currentPageGuard = bpm_ -> FetchPageWrite(GetRootPageId());
  while (!currentPageGuard.template As<BPlusTreePage>() -> IsLeafPage()) {
    const InternalPage* currentInternalPage = currentPageGuard.template As<InternalPage>();
    int index = BinaryFind(currentInternalPage, key);
    parentId.push({currentPageGuard.PageId(), index});
    currentPageGuard = bpm_ -> FetchPageWrite(currentInternalPage -> ValueAt(index));
  }//从当前页找到叶子页
  LeafPage* targetLeaf = currentPageGuard.template AsMut<LeafPage>();
  int targetIndex = BinaryFind(targetLeaf, key);
  if (targetIndex != -1 && comparator_(targetLeaf -> KeyAt(targetIndex), key) == 0) {
    return false;
  }//尝试插入多个相同key,直接返回false
  targetLeaf -> IncreaseSize(1);
  for (int i = targetLeaf -> GetSize() - 1; i > targetIndex + 1; --i) {
    targetLeaf -> SetAt(i, targetLeaf -> KeyAt(i - 1), targetLeaf -> ValueAt(i - 1));
  }
  targetLeaf -> SetAt(targetIndex + 1, key, value);//插入到叶子的键值对中
  if (targetLeaf -> GetMaxSize() < targetLeaf -> GetSize()) {
    int index = targetLeaf -> GetSize() / 2;
    KeyType key = targetLeaf -> KeyAt(index);
    page_id_t newPageId;
    auto newPageGuard = bpm_ -> NewPageGuarded(&newPageId);
    auto writePageGuard = newPageGuard.UpgradeWrite();
    LeafPage* newPage = writePageGuard.template AsMut<LeafPage>();
    newPage -> Init(leaf_max_size_);
    newPage -> SetSize(targetLeaf -> GetSize() - index);
    for (int i = index; i < targetLeaf -> GetSize(); ++i) {
      newPage -> SetAt(i - index, targetLeaf -> KeyAt(i), targetLeaf -> ValueAt(i));
    }
    targetLeaf -> SetSize(index);
    newPage -> SetNextPageId(targetLeaf -> GetNextPageId());//新叶子继承旧叶子的next
    targetLeaf -> SetNextPageId(newPageId);//旧叶子指向新叶子
    if (parentId.empty()) {
      page_id_t newRootId;
      auto newRootGuard = bpm_ -> NewPageGuarded(&newRootId);
      auto headerPageGuard = bpm_ -> FetchPageWrite(header_page_id_);
      auto currentHead = headerPageGuard.template AsMut<BPlusTreeHeaderPage>();
      currentHead -> root_page_id_ = newRootId;
      auto writeRootPageGuard = newRootGuard.UpgradeWrite();
      InternalPage* newRoot = writeRootPageGuard.template AsMut<InternalPage>();
      newRoot -> Init(internal_max_size_);
      newRoot -> SetSize(2);
      newRoot -> SetKeyAt(1, key);
      newRoot -> SetValueAt(1, newPageGuard.PageId());
      newRoot -> SetValueAt(0, currentPageGuard.PageId());
      return true;
    }//情况1: 叶子节点并且是根节点,新建根节点即可直接返回
    page_id_t parent = parentId.top().first;
    int keyPosition = parentId.top().second;
    parentId.pop();
    currentPageGuard = bpm_ -> FetchPageWrite(parent);
    InternalPage* currentPage = currentPageGuard.template AsMut<InternalPage>();
    currentPage -> IncreaseSize(1);
    for (int i = currentPage -> GetSize() - 1; i > keyPosition + 1 ; --i) {
      currentPage -> SetKeyAt(i, currentPage -> KeyAt(i - 1));
      currentPage -> SetValueAt(i, currentPage -> ValueAt(i - 1));
    }
    currentPage -> SetKeyAt(keyPosition + 1, key);
    currentPage -> SetValueAt(keyPosition + 1, newPageId);
    //情况2: 叶子节点不是根节点,向上维护,转情况3或4
    while (currentPage -> GetMaxSize() < currentPage -> GetSize()) {
      int mid = (currentPage -> GetSize()) / 2;
      page_id_t rightPageId;
      auto rightPageGuard = bpm_ -> NewPageGuarded(&rightPageId);
      auto writeRightPage = rightPageGuard.UpgradeWrite();
      InternalPage* rightPage = writeRightPage.template AsMut<InternalPage>();
      rightPage -> Init(internal_max_size_);
      rightPage -> SetSize(currentPage -> GetSize() - mid);
      rightPage -> SetValueAt(0, currentPage -> ValueAt(mid));
      for (int i = 1; i < rightPage -> GetSize(); ++i) {
        rightPage -> SetKeyAt(i, currentPage -> KeyAt(mid + i));
        rightPage -> SetValueAt(i, currentPage -> ValueAt(mid + i));
      }
      KeyType midKey = currentPage -> KeyAt(mid);
      currentPage -> SetSize(mid);
      if (!parentId.empty()) {
        page_id_t parent = parentId.top().first;
        int keyPosition = parentId.top().second;
        parentId.pop();
        currentPageGuard = bpm_ -> FetchPageWrite(parent);
        currentPage = currentPageGuard.template AsMut<InternalPage>();
        currentPage -> IncreaseSize(1);
        for (int i = currentPage -> GetSize() - 1; i > keyPosition + 1 ; --i) {
          currentPage -> SetKeyAt(i, currentPage -> KeyAt(i - 1));
          currentPage -> SetValueAt(i, currentPage -> ValueAt(i - 1));
        }
        currentPage -> SetKeyAt(keyPosition + 1, midKey);
        currentPage -> SetValueAt(keyPosition + 1, rightPageId);
        continue;
      }//情况3: 内部节点并且不是根节点
      page_id_t newRootId;
      auto newRootGuard = bpm_ -> NewPageGuarded(&newRootId);
      auto headerPageGuard = bpm_ -> FetchPageWrite(header_page_id_);
      auto currentHead = headerPageGuard.template AsMut<BPlusTreeHeaderPage>();
      currentHead -> root_page_id_ = newRootId;
      auto writeRootPageGuard = newRootGuard.UpgradeWrite();
      InternalPage* newRoot = writeRootPageGuard.template AsMut<InternalPage>();
      newRoot -> Init(internal_max_size_);
      newRoot -> SetSize(2);
      newRoot -> SetKeyAt(1, midKey);
      newRoot -> SetValueAt(1, rightPageId);
      newRoot -> SetValueAt(0, currentPageGuard.PageId());
      return true;
      //情况4: 内部节点并且是根节点
    }
  }//插入之后的维护
  return true;
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType& key, Transaction* txn)
{
  if (IsEmpty()) {return;}
  std::stack<std::pair<page_id_t, int>> parentId;
  auto currentPageGuard = bpm_ -> FetchPageWrite(GetRootPageId());
  while (!currentPageGuard.template As<BPlusTreePage>() -> IsLeafPage()) {
    const InternalPage* currentInternalPage = currentPageGuard.template As<InternalPage>();
    int index = BinaryFind(currentInternalPage, key);
    parentId.push({currentPageGuard.PageId(), index});
    currentPageGuard = bpm_ -> FetchPageWrite(currentInternalPage -> ValueAt(index));
  }
  LeafPage* targetLeaf = currentPageGuard.template AsMut<LeafPage>();
  int targetIndex = BinaryFind(targetLeaf, key);
  if (targetIndex == -1 || comparator_(targetLeaf -> KeyAt(targetIndex), key) != 0) {
    return;
  }//如果待删除的key不在树中,直接返回
  for (int i = targetIndex + 1; i < targetLeaf -> GetSize(); ++i) {
    targetLeaf -> SetAt(i - 1, targetLeaf -> KeyAt(i), targetLeaf -> ValueAt(i));
  }
  targetLeaf -> IncreaseSize(-1);
  if (targetLeaf -> GetSize() >= targetLeaf -> GetMinSize()) {
    return;
  }//没有触发underflow
  if (parentId.empty()) {
    if (targetLeaf -> GetSize() == 0) {
      auto headerPageGuard = bpm_ -> FetchPageWrite(header_page_id_);
      auto headerPage = headerPageGuard.template AsMut<BPlusTreeHeaderPage>();
      headerPage -> root_page_id_ = INVALID_PAGE_ID;
      bpm_ -> DeletePage(currentPageGuard.PageId());
    }
    return;
  }//当前节点已经是根节点
  page_id_t fatherId = parentId.top().first;
  currentPageGuard = bpm_ -> FetchPageWrite(fatherId);
  InternalPage* currentPage = currentPageGuard.template AsMut<InternalPage>();
  int pos = parentId.top().second;
  parentId.pop();
  if (pos > 0) {
    auto leftPageGuard = bpm_ -> FetchPageWrite(currentPage ->ValueAt(pos - 1));
    LeafPage* leftPage = leftPageGuard.template AsMut<LeafPage>();
    if (leftPage -> GetSize() > leftPage -> GetMinSize()) {
      int vicPos = leftPage -> GetSize() - 1;
      targetLeaf -> IncreaseSize(1);
      for (int i = targetLeaf -> GetSize() - 1; i > 0; --i) {
        targetLeaf -> SetAt(i, targetLeaf -> KeyAt(i - 1), targetLeaf -> ValueAt(i - 1));
      }
      targetLeaf -> SetAt(0, leftPage -> KeyAt(vicPos), leftPage -> ValueAt(vicPos));
      leftPage -> IncreaseSize(-1);
      currentPage -> SetKeyAt(pos, targetLeaf -> KeyAt(0));
      return;
    }
  }
  if (pos <= currentPage -> GetSize() - 2) {
    auto rightPageGuard = bpm_ -> FetchPageWrite(currentPage -> ValueAt(pos + 1));
    LeafPage* rightPage = rightPageGuard.template AsMut<LeafPage>();
    if (rightPage -> GetSize() > rightPage -> GetMinSize()) {
      int vicPos = 0;
      targetLeaf -> IncreaseSize(1);
      targetLeaf -> SetAt(targetLeaf -> GetSize() - 1, rightPage -> KeyAt(vicPos), rightPage -> ValueAt(vicPos));
      for (int i = 0; i < rightPage -> GetSize(); ++i) {
        rightPage -> SetAt(i, targetLeaf -> KeyAt(i + 1), targetLeaf -> ValueAt(i + 1));
      }
      rightPage -> IncreaseSize(-1);
      currentPage -> SetKeyAt(pos + 1, rightPage -> KeyAt(0));
      return;
    }
  }//尝试从左右兄弟redistribute
  if (pos > 0) {
    auto leftPageGuard = bpm_ -> FetchPageWrite(currentPage ->ValueAt(pos - 1));
    LeafPage* leftPage = leftPageGuard.template AsMut<LeafPage>();
    int startPos = leftPage -> GetSize();
    leftPage -> IncreaseSize(targetLeaf -> GetSize());
    for (int i = startPos; i < leftPage -> GetSize(); ++i) {
      leftPage -> SetAt(i, targetLeaf -> KeyAt(i - startPos), targetLeaf -> ValueAt(i - startPos));
    }
    leftPage -> SetNextPageId(targetLeaf -> GetNextPageId());//设置链表
    bpm_ -> DeletePage(currentPage -> ValueAt(pos));
    for (int i = pos; i < currentPage -> GetSize() - 1; ++i){
      currentPage -> SetKeyAt(i, currentPage -> KeyAt(i + 1));
      currentPage -> SetValueAt(i, currentPage -> ValueAt(i + 1));
    }
    currentPage -> IncreaseSize(-1);
  } else {
    auto rightPageGuard = bpm_ -> FetchPageWrite(currentPage -> ValueAt(pos + 1));
    LeafPage* rightPage = rightPageGuard.template AsMut<LeafPage>();
    int startPos = targetLeaf -> GetSize();
    targetLeaf -> IncreaseSize(rightPage -> GetSize());
    for (int i = startPos; i < targetLeaf -> GetSize(); ++i) {
      targetLeaf -> SetAt(i, rightPage -> KeyAt(i - startPos), rightPage -> ValueAt(i - startPos));
    }
    targetLeaf -> SetNextPageId(rightPage -> GetNextPageId());
    bpm_ -> DeletePage(currentPage -> ValueAt(pos + 1));
    for (int i = pos + 1; i < currentPage -> GetSize() - 1; ++i) {
      currentPage -> SetKeyAt(i, currentPage -> KeyAt(i + 1));
      currentPage -> SetValueAt(i, currentPage -> ValueAt(i + 1));
    }
    currentPage -> IncreaseSize(-1);
  }//从左右兄弟合并,转内部节点的调整
  while (currentPage -> GetSize() < currentPage -> GetMinSize()) {
    if (parentId.empty()) {
      if (currentPage -> GetSize() == 1) {
        auto headerPageGuard = bpm_ -> FetchPageWrite(header_page_id_);
        BPlusTreeHeaderPage* headerPage = headerPageGuard.template AsMut<BPlusTreeHeaderPage>();
        headerPage -> root_page_id_ = currentPage -> ValueAt(0);
        bpm_ -> DeletePage(currentPageGuard.PageId());
      }
      return;
    }//如果已经上升到根节点依然underflow,若根只有单独节点则换根,否则返回
    fatherId = parentId.top().first;
    auto nextPageGuard = bpm_ -> FetchPageWrite(fatherId);
    InternalPage* nextPage = nextPageGuard.template AsMut<InternalPage>();
    pos = parentId.top().second;
    parentId.pop();
    if (pos > 0) {
      auto leftPageGuard = bpm_ -> FetchPageWrite(nextPage ->ValueAt(pos - 1));
      InternalPage* leftPage = leftPageGuard.template AsMut<InternalPage>();
      if (leftPage -> GetSize() > leftPage -> GetMinSize()) {
        int vicPos = leftPage -> GetSize() - 1;
        currentPage -> IncreaseSize(1);
        for (int i = currentPage -> GetSize() - 1; i >= 2; --i) {
          currentPage -> SetKeyAt(i, currentPage -> KeyAt(i - 1));
          currentPage -> SetValueAt(i, currentPage -> ValueAt(i - 1));
        }
        currentPage -> SetValueAt(1, currentPage -> ValueAt(0));
        currentPage -> SetKeyAt(1, nextPage -> KeyAt(pos));
        currentPage -> SetValueAt(0, leftPage -> ValueAt(vicPos));
        nextPage -> SetKeyAt(pos, leftPage -> KeyAt(vicPos));
        leftPage -> IncreaseSize(-1);
        return;
      }
    }
    if (pos <= nextPage -> GetSize() - 2) {
      auto rightPageGuard = bpm_ -> FetchPageWrite(nextPage -> ValueAt(pos + 1));
      InternalPage* rightPage = rightPageGuard.template AsMut<InternalPage>();
      if (rightPage -> GetSize() > rightPage -> GetMinSize()) {
        int vicPos = 0;
        currentPage -> IncreaseSize(1);
        currentPage -> SetKeyAt(currentPage -> GetSize() - 1, nextPage -> KeyAt(pos + 1));
        currentPage -> SetValueAt(currentPage -> GetSize() - 1, rightPage -> ValueAt(vicPos));
        nextPage -> SetKeyAt(pos + 1, rightPage -> KeyAt(1));
        rightPage -> SetValueAt(0, rightPage -> ValueAt(1));
        for (int i = 1; i <= rightPage -> GetSize() - 2; ++i) {
          rightPage -> SetKeyAt(i, rightPage -> KeyAt(i + 1));
          rightPage -> SetValueAt(i, rightPage -> ValueAt(i + 1));
        }
        rightPage -> IncreaseSize(-1);
        return;
      }
    }
    if (pos > 0) {
      auto leftPageGuard = bpm_ -> FetchPageWrite(nextPage -> ValueAt(pos - 1));
      InternalPage* leftPage = leftPageGuard.template AsMut<InternalPage>();
      int startPos = leftPage -> GetSize();
      leftPage -> IncreaseSize(targetLeaf -> GetSize());
      leftPage -> SetKeyAt(startPos, nextPage -> KeyAt(pos));
      leftPage -> SetValueAt(startPos, currentPage -> ValueAt(0));
      for (int i = startPos + 1; i < leftPage -> GetSize(); ++i) {
        leftPage -> SetKeyAt(i, currentPage -> KeyAt(i - startPos));
        leftPage -> SetValueAt(i, currentPage -> ValueAt(i - startPos));
      }
      bpm_ -> DeletePage(nextPage -> ValueAt(pos));
      for (int i = pos; i < nextPage -> GetSize() - 1; ++i) {
        nextPage -> SetKeyAt(i, nextPage -> KeyAt(i + 1));
        nextPage -> SetValueAt(i, nextPage -> ValueAt(i + 1));
      }
      nextPage -> IncreaseSize(-1);
      currentPage = nextPage;
    } else {
      auto rightPageGuard = bpm_ -> FetchPageWrite(nextPage -> ValueAt(pos + 1));
      InternalPage* rightPage = rightPageGuard.template AsMut<InternalPage>();
      int startPos = currentPage -> GetSize();
      currentPage -> IncreaseSize(rightPage -> GetSize());
      currentPage -> SetKeyAt(startPos, nextPage -> KeyAt(pos + 1));
      currentPage -> SetValueAt(startPos, rightPage -> ValueAt(0));
      for (int i = startPos + 1; i < currentPage -> GetSize(); ++i) {
        currentPage -> SetKeyAt(i, rightPage -> KeyAt(i - startPos));
        currentPage -> SetValueAt(i, rightPage -> ValueAt(i - startPos));
      }
      bpm_ -> DeletePage(nextPage -> ValueAt(pos + 1));
      for (int i = pos; i < nextPage -> GetSize() - 1; ++i) {
        nextPage -> SetKeyAt(i, nextPage -> KeyAt(pos + 1));
        nextPage -> SetValueAt(i, nextPage -> ValueAt(pos + 1));
      }
      nextPage -> IncreaseSize(-1);
      currentPage = nextPage;
    }
  }//内部节点调整,分类方式与叶子相同,先考虑redistribute,再merge处理
  return;
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const LeafPage* leaf_page, const KeyType& key)
     ->  int
{
  int l = 0;
  int r = leaf_page -> GetSize() - 1;
  while (l < r)
  {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page -> KeyAt(mid), key) != 1)
    {
      l = mid;
    }
    else
    {
      r = mid - 1;
    }
  }

  if (r >= 0 && comparator_(leaf_page -> KeyAt(r), key) == 1)
  {
    r = -1;
  }

  return r;
}//这里的binaryfind找的是最后一个小于等于key的position

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const InternalPage* internal_page,
                                const KeyType& key)  ->  int
{
  int l = 1;
  int r = internal_page -> GetSize() - 1;
  while (l < r)
  {
    int mid = (l + r + 1) >> 1;
    if (comparator_(internal_page -> KeyAt(mid), key) != 1)
    {
      l = mid;
    }
    else
    {
      r = mid - 1;
    }
  }

  if (r == -1 || comparator_(internal_page -> KeyAt(r), key) == 1)
  {
    r = 0;
  }

  return r;
}

/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin()  ->  INDEXITERATOR_TYPE
//Just go left forever
{
  ReadPageGuard head_guard = bpm_ -> FetchPageRead(header_page_id_);
  if (head_guard.template As<BPlusTreeHeaderPage>() -> root_page_id_ == INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard = bpm_ -> FetchPageRead(head_guard.As<BPlusTreeHeaderPage>() -> root_page_id_);
  head_guard.Drop();

  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page -> IsLeafPage())
  {
    int slot_num = 0;
    guard = bpm_ -> FetchPageRead(reinterpret_cast<const InternalPage*>(tmp_page) -> ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  int slot_num = 0;
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
  }
  return End();
}


/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType& key)  ->  INDEXITERATOR_TYPE
{
  ReadPageGuard head_guard = bpm_ -> FetchPageRead(header_page_id_);

  if (head_guard.template As<BPlusTreeHeaderPage>() -> root_page_id_ == INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard = bpm_ -> FetchPageRead(head_guard.As<BPlusTreeHeaderPage>() -> root_page_id_);
  head_guard.Drop();
  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page -> IsLeafPage())
  {
    auto internal = reinterpret_cast<const InternalPage*>(tmp_page);
    int slot_num = BinaryFind(internal, key);
    if (slot_num == -1)
    {
      return End();
    }
    guard = bpm_ -> FetchPageRead(reinterpret_cast<const InternalPage*>(tmp_page) -> ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);

  int slot_num = BinaryFind(leaf_page, key);
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End()  ->  INDEXITERATOR_TYPE
{
  return INDEXITERATOR_TYPE(bpm_, -1, -1);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId()  ->  page_id_t
{
  ReadPageGuard guard = bpm_ -> FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = root_header_page -> root_page_id_;
  return root_page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string& file_name,
                                      Transaction* txn)
{
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input)
  {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction)
    {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager* bpm)
{
  auto root_page_id = GetRootPageId();
  auto guard = bpm -> FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage* page)
{
  if (page -> IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf -> GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf -> GetSize(); i++)
    {
      std::cout << leaf -> KeyAt(i);
      if ((i + 1) < leaf -> GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  else
  {
    auto* internal = reinterpret_cast<const InternalPage*>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal -> GetSize(); i++)
    {
      std::cout << internal -> KeyAt(i) << ": " << internal -> ValueAt(i);
      if ((i + 1) < internal -> GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal -> GetSize(); i++)
    {
      auto guard = bpm_ -> FetchPageBasic(internal -> ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager* bpm, const std::string& outf)
{
  if (IsEmpty())
  {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm -> FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage* page,
                             std::ofstream& out)
{
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page -> IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">"
        << "max_size=" << leaf -> GetMaxSize()
        << ",min_size=" << leaf -> GetMinSize() << ",size=" << leaf -> GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf -> GetSize(); i++)
    {
      out << "<TD>" << leaf -> KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf -> GetNextPageId() != INVALID_PAGE_ID)
    {
      out << leaf_prefix << page_id << "   ->   " << leaf_prefix
          << leaf -> GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix
          << leaf -> GetNextPageId() << "};\n";
    }
  }
  else
  {
    auto* inner = reinterpret_cast<const InternalPage*>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner -> GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner -> GetSize() << "\">"
        << "max_size=" << inner -> GetMaxSize()
        << ",min_size=" << inner -> GetMinSize() << ",size=" << inner -> GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner -> GetSize(); i++)
    {
      out << "<TD PORT=\"p" << inner -> ValueAt(i) << "\">";
      // if (i > 0) {
      out << inner -> KeyAt(i) << "  " << inner -> ValueAt(i);
      // } else {
      // out << inner  ->  ValueAt(0);
      // }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner -> GetSize(); i++)
    {
      auto child_guard = bpm_ -> FetchPageBasic(inner -> ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0)
      {
        auto sibling_guard = bpm_ -> FetchPageBasic(inner -> ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page -> IsLeafPage() && !child_page -> IsLeafPage())
        {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId()
              << " " << internal_prefix << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId()
          << "   ->   ";
      if (child_page -> IsLeafPage())
      {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      }
      else
      {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree()  ->  std::string
{
  if (IsEmpty())
  {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id)
     ->  PrintableBPlusTree
{
  auto root_page_guard = bpm_ -> FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page -> IsLeafPage())
  {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page -> ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page -> ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page -> GetSize(); i++)
  {
    page_id_t child_id = internal_page -> ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub