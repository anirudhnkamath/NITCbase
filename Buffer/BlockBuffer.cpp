#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <stdio.h>

BlockBuffer::BlockBuffer(int blockNum){
  this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

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

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** bufferPtr){
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if(bufferNum == E_BLOCKNOTINBUFFER){
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if(bufferNum == E_OUTOFBOUND) return E_OUTOFBOUND;

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  *bufferPtr = StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}
