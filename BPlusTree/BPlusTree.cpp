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
