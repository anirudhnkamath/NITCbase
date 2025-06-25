#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <stdio.h>

// constructor 2 (if block already exists)
BlockBuffer::BlockBuffer(int blockNum){
  this->blockNum = blockNum;
}

// constructor 1 (if block doesnt exist)
BlockBuffer::BlockBuffer(char blockType) {
  int blockTypeInt;
  if(blockType == 'R') blockTypeInt = REC;
  else if(blockType == 'I') blockTypeInt = IND_INTERNAL;
  else blockTypeInt = IND_LEAF;

  // sets this->blocknum as a free block (doesnt check if its valid block or error code)
  int getBlockRes = this->getFreeBlock(blockTypeInt);
  this->blockNum = getBlockRes;
}

// loads header of a block
int BlockBuffer::getHeader(struct HeadInfo* head){
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);

  if(ret != SUCCESS) return ret;

  memcpy(&head->blockType, bufferPtr + 0, 4);
  memcpy(&head->pblock, bufferPtr + 4, 4);
  memcpy(&head->lblock, bufferPtr + 8, 4);
  memcpy(&head->rblock, bufferPtr + 12, 4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs, bufferPtr + 20, 4);
  memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->reserved, bufferPtr + 28, 4);

  return SUCCESS;
}

// returns the staticbuffer pointer of given block (loads it into buffer if doesnt exist in buffer)
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** bufferPtr){
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
  
  if(bufferNum == E_OUTOFBOUND)
    return E_OUTOFBOUND;
  
  // if block in buffer, updates the timestamps of all blocks
  if(bufferNum != E_BLOCKNOTINBUFFER) {
    // timestamp = 0 for selected block, +1 for others
    for(int i=0; i<BUFFER_CAPACITY; i++) {
      if(i == bufferNum) StaticBuffer::metainfo[i].timeStamp = 0;
      else StaticBuffer::metainfo[i].timeStamp += 1;
    }
  }
  
  // if block not in buffer, reads gets free slot and then reads from disk
  else {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
    if(bufferNum == E_OUTOFBOUND)
      return E_OUTOFBOUND;

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // returns buffer pointer
  *bufferPtr = StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}

// sets the header of a block
int BlockBuffer::setHeader(struct HeadInfo* head) {
  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  // sets the struct values
  struct HeadInfo* bufferHeader = (struct HeadInfo*)bufferPtr;
  bufferHeader[0].blockType = head->blockType;
  bufferHeader[0].lblock = head->lblock;
  bufferHeader[0].numAttrs = head->numAttrs;
  bufferHeader[0].numEntries = head->numEntries;
  bufferHeader[0].numSlots = head->numSlots;
  bufferHeader[0].pblock = head->pblock;
  bufferHeader[0].rblock = head->rblock;

  // since buffer is modifies, sets the dirty bit to true
  int setDirtyRes = StaticBuffer::setDirtyBit(this->blockNum);
  if(setDirtyRes != SUCCESS)
    return setDirtyRes;

  return SUCCESS;
}

// sets the block type in header of block
int BlockBuffer::setBlockType(int blockType) {
  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  // sets block type in header
  ((int32_t*)bufferPtr)[0] = blockType;
  // updates blockallocation map to match the type
  StaticBuffer::blockAllocMap[this->blockNum] = blockType;
  
  // since buffer is modified, sets it as dirty
  int setDirtyRes = StaticBuffer::setDirtyBit(this->blockNum);
  if(setDirtyRes != SUCCESS)
    return setDirtyRes;

  return SUCCESS;
}

// gets a free block and sets the header of the empty block
int BlockBuffer::getFreeBlock(int blockType) {
  
  // iterate through bMAP
  int allocatedBlockNum = -1;
  for(int i=0; i<DISK_BLOCKS; i++) {
    if(StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
      allocatedBlockNum = i;
      break;
    }
  }

  if(allocatedBlockNum == -1)
    return E_DISKFULL;

  // set the header 
  this->blockNum = allocatedBlockNum;

  struct HeadInfo head;
  head.pblock = -1;
  head.lblock = -1;
  head.rblock = -1;
  head.numAttrs = 0;
  head.numEntries = 0;
  head.numSlots = 0;
  this->setHeader(&head);

  // set the blocktype
  this->setBlockType(blockType);

  return allocatedBlockNum;
} 

// returns the blocknum of the buffer
int BlockBuffer::getBlockNum() {
  return this->blockNum;
}

// sets the block number in blockbuffer as INVALID BLOCK NUM
void BlockBuffer::releaseBlock() {
  if(this->blockNum == INVALID_BLOCKNUM)
    return;

  int bufferIndex = StaticBuffer::getBufferNum(this->blockNum);
  
  // if in buffer, frees it and sets free = true
  if(bufferIndex != E_BLOCKNOTINBUFFER)
    StaticBuffer::metainfo[bufferIndex].free = true;
  
  // updates block allocation map
  StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
  this->blockNum = INVALID_BLOCKNUM;
}



// constructor 2 (if block exists)
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// constructor 1 (if block doesnt exist)
RecBuffer::RecBuffer() : BlockBuffer::BlockBuffer('R'){};

// gets the record of the block, and given slot
int RecBuffer::getRecord(union Attribute *rec, int slotNum){
  struct HeadInfo head;
  this->getHeader(&head);

  unsigned char * bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) return ret;

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  int recordSize = attrCount * ATTR_SIZE;

  unsigned char* slotPointer = bufferPtr + HEADER_SIZE + slotCount + (recordSize * slotNum);
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

// sets the record of a block, and given slot
int RecBuffer::setRecord(union Attribute* rec, int slotNum) {
  unsigned char* bufferPtr;
  int loadResult = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadResult != SUCCESS)
    return loadResult;

  HeadInfo head;
  getHeader(&head);
  int numSlots = head.numSlots;
  int numAttrs = head.numAttrs;

  if(slotNum < 0 || slotNum >= numSlots)
    return E_OUTOFBOUND;

  int offset = HEADER_SIZE + numSlots + (numAttrs * ATTR_SIZE * slotNum);
  memcpy(bufferPtr + offset, rec, ATTR_SIZE * numAttrs);

  // updates dirty bit as buffer was modified
  StaticBuffer::setDirtyBit(this->blockNum);
  return SUCCESS;
}

// gets the slot map of the block
int RecBuffer::getSlotMap(unsigned char* slotMap) {
  unsigned char* bufferPtr;

  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  
  struct HeadInfo head;
  this->getHeader(&head);
  int slotCount = head.numSlots;

  unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;
  memcpy(slotMap, slotMapInBuffer, slotCount);

  return SUCCESS;
}

// sets the slot map of the block
int RecBuffer::setSlotMap(unsigned char* slotMap) {
  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  HeadInfo head;
  this->getHeader(&head);
  int numSlots = head.numSlots;
  memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);

  // dirty bit set as buffer modified
  int dirtyRes = StaticBuffer::setDirtyBit(this->blockNum);

  if(dirtyRes != SUCCESS)
    return dirtyRes;
  return SUCCESS;
}



IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}



IndInternal::IndInternal() : IndBuffer('I') {}

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

int IndInternal::getEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
    return E_OUTOFBOUND;

  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  struct InternalEntry* internalEntry = (struct InternalEntry*)ptr;
  unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

  memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
  memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
  memcpy(&(internalEntry->rChild), entryPtr + 20, sizeof(int32_t));
  
  return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
    return E_OUTOFBOUND;

  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  struct InternalEntry* internalEntry = (struct InternalEntry*)ptr;
  unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

  memcpy(entryPtr, &(internalEntry->lChild), 4);
  memcpy(entryPtr + 4, &(internalEntry->attrVal), ATTR_SIZE);
  memcpy(entryPtr + 20, &(internalEntry->rChild), 4);

  int dirtyRes = StaticBuffer::setDirtyBit(this->blockNum);
  if(dirtyRes != SUCCESS)
    return dirtyRes;
  
  return SUCCESS;
}



IndLeaf::IndLeaf() : IndBuffer('L') {}

IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

int IndLeaf::getEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
    return E_OUTOFBOUND;

  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);;
  if(loadRes != SUCCESS)
    return loadRes;

  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
  memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

  return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
    return E_OUTOFBOUND;

  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);;
  if(loadRes != SUCCESS)
    return loadRes;

  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
  memcpy(entryPtr, (struct Index *)ptr, LEAF_ENTRY_SIZE);

  int dirtyRes = StaticBuffer::setDirtyBit(this->blockNum);
  if(dirtyRes != SUCCESS)
    return dirtyRes;
  
  return SUCCESS;
}



// utility function to compare 2 attributes
int compareAttrs(Attribute attr1, Attribute attr2, int attrType) {
  double diff;
  if(attrType == STRING)
    diff = strcmp(attr1.sVal, attr2.sVal);
  else
    diff = attr1.nVal - attr2.nVal;

  if(diff < 0) return -1;
  else if(diff == 0) return 0;
  else return 1;
}