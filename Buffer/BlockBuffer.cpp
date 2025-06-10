#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <stdio.h>

BlockBuffer::BlockBuffer(int blockNum){
  this->blockNum = blockNum;
}

BlockBuffer::BlockBuffer(char blockType) {
  int blockTypeInt;
  if(blockType == 'R') blockTypeInt = REC;
  else if(blockType == 'I') blockTypeInt = IND_INTERNAL;
  else if(blockType == 'L') blockTypeInt = IND_LEAF;

  int getBlockRes = this->getFreeBlock(blockTypeInt);
  this->blockNum = getBlockRes;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

RecBuffer::RecBuffer() : BlockBuffer::BlockBuffer('R'){};


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

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** bufferPtr){
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
  
  if(bufferNum != E_BLOCKNOTINBUFFER) {
    for(int i=0; i<BUFFER_CAPACITY; i++) {
      if(i == bufferNum) StaticBuffer::metainfo[i].timeStamp = 0;
      else StaticBuffer::metainfo[i].timeStamp += 1;
    }
  }
  
  else {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
    if(bufferNum == E_OUTOFBOUND)
      return E_OUTOFBOUND;

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  *bufferPtr = StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo* head) {
  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  struct HeadInfo* bufferHeader = (struct HeadInfo*)bufferPtr;
  bufferHeader->blockType = head->blockType;
  bufferHeader->lblock = head->lblock;
  bufferHeader->numAttrs = head->numAttrs;
  bufferHeader->numEntries = head->numEntries;
  bufferHeader->numSlots = head->numSlots;
  bufferHeader->pblock = head->pblock;
  bufferHeader->rblock = head->rblock;

  int setDirtyRes = StaticBuffer::setDirtyBit(this->blockNum);
  if(setDirtyRes != SUCCESS)
    return setDirtyRes;

  return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType) {
  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  // set block type in header;
  *(int32_t*)bufferPtr = blockType;

  StaticBuffer::blockAllocMap[this->blockNum] = blockType;
  
  int setDirtyRes = StaticBuffer::setDirtyBit(this->blockNum);
  if(setDirtyRes != SUCCESS)
    return setDirtyRes;

  return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType) {
  int allocatedBlockNum = -1;
  for(int i=0; i<DISK_BLOCKS; i++) {
    if(StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
      allocatedBlockNum = i;
      break;
    }
  }

  if(allocatedBlockNum == -1)
    return E_DISKFULL;

  this->blockNum = allocatedBlockNum;
  int bufferIndex = StaticBuffer::getFreeBuffer(this->blockNum);
  struct HeadInfo head;
  head.pblock = -1;
  head.lblock = -1;
  head.rblock = -1;
  head.numAttrs = 0;
  head.numEntries = 0;
  head.numSlots = 0;

  this->setHeader(&head);
  this->setBlockType(blockType);

  return allocatedBlockNum;
} 

int BlockBuffer::getBlockNum() {
  return this->blockNum;
}


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

  StaticBuffer::setDirtyBit(this->blockNum);
  return SUCCESS;
}

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

int RecBuffer::setSlotMap(unsigned char* slotMap) {
  unsigned char* bufferPtr;
  int loadRes = loadBlockAndGetBufferPtr(&bufferPtr);
  if(loadRes != SUCCESS)
    return loadRes;

  HeadInfo head;
  this->getHeader(&head);
  int numSlots = head.numSlots;

  memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);
  int dirtyRes = StaticBuffer::setDirtyBit(this->blockNum);

  if(dirtyRes != SUCCESS)
    return dirtyRes;
  return SUCCESS;
}

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