#include "BPlusTree.h"
#include <stdio.h>
#include <cstring>

// search relation using b plus tree and find ONLY the next match and return
RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {

  // get the last hit search index
  IndexId lastSearchIndex;
  AttrCacheTable::getSearchIndex(relId, attrName, &lastSearchIndex);
  
  AttrCatEntry attrCatEntry;
  AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
  int attrType = attrCatEntry.attrType;

  // initialise the block and index to traverse
  int block, index;

  // if {-1, -1} start from beginning
  if(lastSearchIndex.block == -1 && lastSearchIndex.index == -1) {
    block = attrCatEntry.rootBlock;
    index = 0;

    if(block == -1)
      return RecId{-1, -1};
  }

  // else start from next slot
  else {
    block = lastSearchIndex.block;
    index = lastSearchIndex.index + 1;

    IndLeaf leafBuffer(block);
    HeadInfo leafHead;
    leafBuffer.getHeader(&leafHead);

    // move to next block, if current block is over
    if(index >= leafHead.numEntries) {
      block = leafHead.rblock;
      index = 0;
      if(block == -1) 
        return RecId{-1, -1};
    }
  }

  // iterate through leaf blocks starting from root (only enters this if its not a leaf)
  while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {

    // create buffer for current internal node
    IndInternal indBlockBuffer(block);
    HeadInfo indBlockHead;
    indBlockBuffer.getHeader(&indBlockHead);

    InternalEntry intEntry;

    // for these operations, first occurence will be always at leftmost leaf, so always move left
    if(op == NE || op == LT || op == LE) {
      indBlockBuffer.getEntry(&intEntry, 0);
      block = intEntry.lChild;
    }

    // for other operations, move to left of the first entry greater than given value
    else {
      int numEntries = indBlockHead.numEntries;

      bool found = false;
      for(int i=0; i<numEntries; i++) {
        indBlockBuffer.getEntry(&intEntry, i);

        // case for >= or =
        if(op == EQ || op == GE) {
          if(compareAttrs(intEntry.attrVal, attrVal, attrType) >= 0) {
            found = true;
            block = intEntry.lChild;
            break;
          }
        }

        // case for >
        else {
          if(compareAttrs(intEntry.attrVal, attrVal, attrType) > 0) {
            found = true;
            block = intEntry.lChild;
            break;
          }
        }
      }

      // if no entry is greater, move to last entry's rightchild
      if(!found) {
        indBlockBuffer.getEntry(&intEntry, numEntries-1);
        block = intEntry.rChild;
      }
    }
    
  }

  // now we found a LEAF node
  
  // iterate through all leaf nodes starting from the one we found
  while(block != -1) {
    IndLeaf leafBlockBuffer(block);
    HeadInfo leafBlockHead;
    leafBlockBuffer.getHeader(&leafBlockHead);
    int numEntries = leafBlockHead.numEntries;

    // iterate through current leaf node
    while(index < numEntries) {
      Index leafEntry;
      leafBlockBuffer.getEntry(&leafEntry, index);

      int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrType);

      // if found, return it and change search index
      if(
        (op == EQ && cmpVal == 0) ||
        (op == LE && cmpVal <= 0) ||
        (op == LT && cmpVal < 0) ||
        (op == GT && cmpVal > 0) ||
        (op == GE && cmpVal >= 0) ||
        (op == NE && cmpVal != 0)
      ) {
        IndexId foundIndexId = {block, index};
        AttrCacheTable::setSearchIndex(relId, attrName, &foundIndexId);
        return RecId{leafEntry.block, leafEntry.slot};
      }
      // for these operations, we will never find a suitable record later on, hence return
      else if((op == EQ || op == LE || op == LT) && cmpVal > 0) {
        return RecId{-1, -1};
      }

      index += 1;
    }

    // for all operations other than ne, its guaranteed to find result in 1 lead node only
    if(op != NE)
      break;

    // next block
    block = leafBlockHead.rblock;
    index = 0;
  }

  // nothing found 
  return RecId{-1, -1};
} 

// recursively releases all blocks of the bplus tree and sets rootblk = -1
int BPlusTree::bPlusDestroy(int rootBlock) {
  if(rootBlock < 0 || rootBlock >= DISK_BLOCKS)
    return E_OUTOFBOUND;
  
  int type = StaticBuffer::getStaticBlockType(rootBlock);

  // base case (destroy the leaf)
  if(type == IND_LEAF) {
    IndLeaf leafBuffer(rootBlock);
    leafBuffer.releaseBlock();
    return SUCCESS;
  }

  // recusive case (calls the children)
  else if(type == IND_INTERNAL) {
    IndInternal intBuffer(rootBlock);

    HeadInfo curHead;
    intBuffer.getHeader(&curHead);
    int numEntries = curHead.numEntries;

    for(int i=0; i<numEntries; i++) {
      InternalEntry intEntry;
      intBuffer.getEntry(&intEntry, i);

      // delete the left child of 1st entry and right child of all entries
      if(i == 0) BPlusTree::bPlusDestroy(intEntry.lChild);
      BPlusTree::bPlusDestroy(intEntry.rChild);
    }

    intBuffer.releaseBlock();
    return SUCCESS;
  }

  // invalid block
  else {
    return E_INVALIDBLOCK;
  }
}

// creates bplus tree by repeatedly inserting all records of the relation into the tree
int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {
  if(relId == RELCAT_RELID || relId == ATTRCAT_RELID)
    return E_NOTPERMITTED;
  
  // check if attribute is valid and if index already exists
  AttrCatEntry attrCatEntry;
  int attrRes = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
  if(attrRes != SUCCESS)
    return attrRes;

  if(attrCatEntry.rootBlock != -1)
    return SUCCESS;

  // creating the bplus tree

  IndLeaf rootBlockBuf;

  // check if root was successfully created
  int rootBlock = rootBlockBuf.getBlockNum();
  if(rootBlock == E_DISKFULL)
    return E_DISKFULL;
  
  // set the rootblock in attrcatentry
  attrCatEntry.rootBlock = rootBlock;
  AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId, &relCatEntry);

  // iterate through all records blocks of the relation
  int block = relCatEntry.firstBlk;
  while(block != -1) {
    RecBuffer curBlockBuffer(block);

    int numSlots = relCatEntry.numSlotsPerBlk;
    int numAttrs = relCatEntry.numAttrs;

    unsigned char slotMap[numSlots];
    curBlockBuffer.getSlotMap(slotMap);

    // for all occupied slots of the block
    for(int slot=0; slot<numSlots; slot++) {
      if(slotMap[slot] == SLOT_OCCUPIED) {
        
        // get the record at slot
        Attribute record[numAttrs];
        curBlockBuffer.getRecord(record, slot);

        int attrOffset = attrCatEntry.offset;
        // prepare the input for bplusinsert function, and insert 
        RecId recId{block, slot};
        Attribute attrVal = record[attrOffset];
        int insertRes = BPlusTree::bPlusInsert(relId, attrName, attrVal, recId);

        if(insertRes == E_DISKFULL)
          return E_DISKFULL;
      }
    }

    // move to next block
    HeadInfo curBlockHeader;
    curBlockBuffer.getHeader(&curBlockHeader);
    block = curBlockHeader.rblock;
  }

  return SUCCESS;
}

// highest level function to insert a key to a bplus tree
int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
  AttrCatEntry attrCatEntry;
  int getAttrRes = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
  if(getAttrRes != SUCCESS)
    return getAttrRes;
  
  int blockNum = attrCatEntry.rootBlock;
  if(blockNum == -1)
    return E_NOINDEX;
  
  int attrType = attrCatEntry.attrType;
  // find a leaf node to insert
  int leafBlockNum = findLeafToInsert(blockNum, attrVal, attrType);

  // create a leaf entry and call inserttoleaf function
  Index curEntry;
  curEntry.attrVal = attrVal, curEntry.block = recId.block, curEntry.slot = recId.slot;
  int insertRes = insertIntoLeaf(relId, attrName, leafBlockNum, curEntry);

  // if insert fails, destroy the whole bptree
  if(insertRes == E_DISKFULL) {
    bPlusDestroy(blockNum);

    attrCatEntry.rootBlock = -1;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    return E_DISKFULL;
  }

  return SUCCESS;
}

// finds a suitable leaf block to insert
int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
  int blockNum = rootBlock;

  // iterates downwards through internal blocks till it reaches leaf node
  while(StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {
    IndInternal intBuffer(blockNum);
    HeadInfo intHeader;
    intBuffer.getHeader(&intHeader);

    // finds correct child pointer to move to
    int foundIndex = -1;
    for(int index=0; index<intHeader.numEntries; index++) {
      InternalEntry intEntry;
      intBuffer.getEntry(&intEntry, index);

      // find the first entry whose value >= value to be inserted
      if(compareAttrs(intEntry.attrVal, attrVal, attrType) > 0) {
        foundIndex = index;
        break;
      }
    }

    // if all internal values are smaller than given value, move to righmost child
    if(foundIndex == -1) {
      InternalEntry intEntry;
      intBuffer.getEntry(&intEntry, intHeader.numEntries-1);
      blockNum = intEntry.rChild;
    }
    // else move to left of the found entry
    else {
      InternalEntry intEntry;
      intBuffer.getEntry(&intEntry, foundIndex);
      blockNum = intEntry.lChild;
    }
  }

  return blockNum;
}

// inserts into leaf and handles the edge cases of being a full leaf
int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
  AttrCatEntry attrCatEntry;
  AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
  int attrType = attrCatEntry.attrType;

  IndLeaf leafBuffer(blockNum);
  HeadInfo leafHeader;
  leafBuffer.getHeader(&leafHeader);

  // insert all indexes in the leaf to an array
  Index indices[leafHeader.numEntries + 1];
  for(int i=0; i<leafHeader.numEntries; i++) {
    Index curEntry;
    leafBuffer.getEntry(&curEntry, i);
    indices[i] = curEntry;
  }

  // find the right spot to insert indexEntry such that the array remains sorted
  int arrayInsertIndex;
  for(arrayInsertIndex = leafHeader.numEntries - 1; arrayInsertIndex >= 0; arrayInsertIndex--) {
      if(compareAttrs(indices[arrayInsertIndex].attrVal, indexEntry.attrVal, attrType) > 0) {
          indices[arrayInsertIndex + 1] = indices[arrayInsertIndex];
      } else {
          break;
      }
  }
  arrayInsertIndex += 1;
  indices[arrayInsertIndex] = indexEntry;

  // if leaf was not full before insertion, hence we can directly copy the array to the leaf
  if(leafHeader.numEntries != MAX_KEYS_LEAF) {
    leafHeader.numEntries += 1;
    leafBuffer.setHeader(&leafHeader);

    for(int i=0; i<leafHeader.numEntries; i++)
      leafBuffer.setEntry(&indices[i], i);
    
    return SUCCESS;
  }

  // else if it was already full before insertion
  
  // split the current block and create a new right leaf block
  int newRightLeafNum = splitLeaf(blockNum, indices);
  if(newRightLeafNum == E_DISKFULL)
    return E_DISKFULL;

  InternalEntry entryToParent;
  entryToParent.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
  entryToParent.lChild = blockNum;
  entryToParent.rChild = newRightLeafNum;

  int retVal;
  // if current leaf was not the root node
  if(leafHeader.pblock != -1) {
    retVal = insertIntoInternal(relId, attrName, leafHeader.pblock, entryToParent);
  }
  // the leaf was the root node
  else
    retVal = createNewRoot(relId, attrName, entryToParent.attrVal, blockNum, newRightLeafNum);

  if(retVal == E_DISKFULL)
    return E_DISKFULL;

  return SUCCESS;
}

// split a full leaf node and divide the entries between 2 leaf nodes and set suitable headers
int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
  IndLeaf leftLeaf(leafBlockNum);
  IndLeaf rightLeaf;
  
  int leftLeafNum = leftLeaf.getBlockNum();
  int rightLeafNum = rightLeaf.getBlockNum();

  if(rightLeafNum == E_DISKFULL)
    return E_DISKFULL;

  HeadInfo leftLeafHead, rightLeafHead;
  leftLeaf.getHeader(&leftLeafHead);
  rightLeaf.getHeader(&rightLeafHead);

  // set the header of right leaf
  rightLeafHead.numEntries = (MAX_KEYS_LEAF + 1)/2;
  rightLeafHead.pblock = leftLeafHead.pblock;
  rightLeafHead.lblock = leftLeafNum;
  rightLeafHead.rblock = leftLeafHead.rblock;
  rightLeaf.setHeader(&rightLeafHead);

  // set the header of left leaf
  leftLeafHead.numEntries = (MAX_KEYS_LEAF+1)/2;
  leftLeafHead.rblock = rightLeafNum;
  leftLeaf.setHeader(&leftLeafHead);

  // set the entries of left leaf
  for(int i=0; i<32; i++)
    leftLeaf.setEntry(&indices[i], i);
  // set entries of right leaf
  for(int i=32; i<64; i++)
    rightLeaf.setEntry(&indices[i], i-32);

  return rightLeafNum;
}

// inserts to internal node and handles edges case when internal node is fulll
int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {
  AttrCatEntry attrCatEntry;
  AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
  int attrType = attrCatEntry.attrType;

  IndInternal intBuffer(intBlockNum);
  HeadInfo intHeader;
  intBuffer.getHeader(&intHeader);

  // insert all indexes in the internal block to an array
  InternalEntry intEntries[intHeader.numEntries + 1];
  for(int i=0; i<intHeader.numEntries; i++) {
    InternalEntry curEntry;
    intBuffer.getEntry(&curEntry, i);
    intEntries[i] = curEntry;
  }

  // find the right spot to insert intEntry such that the array remains sorted
  int arrayInsertIndex;
  for(arrayInsertIndex = intHeader.numEntries - 1; arrayInsertIndex >= 0; arrayInsertIndex--) {
      if(compareAttrs(intEntries[arrayInsertIndex].attrVal, intEntry.attrVal, attrType) > 0) {
          intEntries[arrayInsertIndex + 1] = intEntries[arrayInsertIndex];
      } else {
          break;
      }
  }
  arrayInsertIndex += 1;
  intEntries[arrayInsertIndex] = intEntry;
  
  // change the lchild and rchild of neighbouring entries
  if(arrayInsertIndex < intHeader.numEntries)
    intEntries[arrayInsertIndex+1].lChild = intEntry.rChild;
  if(arrayInsertIndex > 0)
    intEntries[arrayInsertIndex-1].rChild = intEntry.lChild;

  // if internal node wasnt full before we added the new entry
  if(intHeader.numEntries != MAX_KEYS_INTERNAL) {
    intHeader.numEntries += 1;
    intBuffer.setHeader(&intHeader);

    for(int i=0; i<intHeader.numEntries; i++)
      intBuffer.setEntry(&intEntries[i], i);
    
    return SUCCESS;
  }
  
  // else if it was full already

  // split the internal node to 2
  int newRightInternal = splitInternal(intBlockNum, intEntries);

  // if splitting failed due to disk full, delete the right tree of intentry
  if(newRightInternal == E_DISKFULL) {
    bPlusDestroy(intEntry.rChild);
    return E_DISKFULL;
  }
  
  // create a new entry for the parent
  InternalEntry intEntryToParent;
  intEntryToParent.attrVal = intEntries[MIDDLE_INDEX_INTERNAL].attrVal;
  intEntryToParent.lChild = intBlockNum;
  intEntryToParent.rChild = newRightInternal;

  int insertRes;
  if(intHeader.pblock != -1) 
    insertRes = insertIntoInternal(relId, attrName, intHeader.pblock, intEntryToParent);
  else
    insertRes = createNewRoot(relId, attrName, intEntryToParent.attrVal, intBlockNum, newRightInternal);

  if(insertRes != SUCCESS) 
    return insertRes;
  return SUCCESS;
}

// splits a full internal node and sets the suitable headers
int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
  IndInternal rightInternal;
  IndInternal leftInternal(intBlockNum);

  int rightBlockNum = rightInternal.getBlockNum();
  int leftBlockNum = leftInternal.getBlockNum();

  if(rightBlockNum == E_DISKFULL)
    return E_DISKFULL;

  HeadInfo leftHead, rightHead;
  leftInternal.getHeader(&leftHead);
  rightInternal.getHeader(&rightHead);

  // set headers of left and right blocks
  rightHead.numEntries = (MAX_KEYS_INTERNAL)/2;
  rightHead.pblock = leftHead.pblock;
  rightInternal.setHeader(&rightHead);
  
  leftHead.numEntries = (MAX_KEYS_INTERNAL)/2;
  leftInternal.setHeader(&leftHead);

  // set the entries in left and right block
  for(int i=0; i<50; i++) 
    leftInternal.setEntry(&internalEntries[i], i);
  for(int i=51; i<101; i++)
    rightInternal.setEntry(&internalEntries[i], i-51);

  // set the parent of all children of rightblock to "rightblocknum"
  for(int i=51; i<101; i++) {
    if(i == 51) {
      BlockBuffer childBuffer(internalEntries[i].lChild);
      HeadInfo head;
      childBuffer.getHeader(&head);
      head.pblock = rightBlockNum;
      childBuffer.setHeader(&head);
    }
    BlockBuffer childBuffer(internalEntries[i].rChild);
    HeadInfo head;
    childBuffer.getHeader(&head);
    head.pblock = rightBlockNum;
    childBuffer.setHeader(&head);
  }

  return rightBlockNum;
}

// creates a new root
int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
  AttrCatEntry attrCatEntry;
  AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

  IndInternal newRootBlock;
  int rootNum = newRootBlock.getBlockNum();

  if(rootNum == E_DISKFULL) {
    bPlusDestroy(rChild);
    return E_DISKFULL;
  }

  // set entries as 1
  HeadInfo rootHead;
  newRootBlock.getHeader(&rootHead);
  rootHead.numEntries = 1;
  newRootBlock.setHeader(&rootHead);

  InternalEntry newEntry;
  newEntry.lChild = lChild, newEntry.rChild = rChild, newEntry.attrVal = attrVal;
  newRootBlock.setEntry(&newEntry, 0);

  // set the parent of lchild and rchild to current new root block
  BlockBuffer lChildBuffer(lChild);
  HeadInfo lChildHead;
  lChildBuffer.getHeader(&lChildHead);
  lChildHead.pblock = rootNum;
  lChildBuffer.setHeader(&lChildHead);
  BlockBuffer rChildBuffer(rChild);
  HeadInfo rChildHead;
  rChildBuffer.getHeader(&rChildHead);
  rChildHead.pblock = rootNum;
  rChildBuffer.setHeader(&rChildHead);

  // changes rootblock value in cache
  attrCatEntry.rootBlock = rootNum;
  AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
  
  return SUCCESS;
}