#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <stdio.h>

BlockBuffer::BlockBuffer(int blockNum){
  this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

int BlockBuffer::getHeader(struct HeadInfo* head){
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  memcpy(&head->blockType, buffer + 0, 4);
  memcpy(&head->pblock, buffer + 4, 4);
  memcpy(&head->lblock, buffer + 8, 4);
  memcpy(&head->rblock, buffer + 12, 4);
  memcpy(&head->numEntries, buffer + 16, 4);
  memcpy(&head->numAttrs, buffer + 20, 4);
  memcpy(&head->numSlots, buffer + 24, 4);
  memcpy(&head->reserved, buffer + 28, 4);

  return SUCCESS;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum){
  struct HeadInfo head;
  this->getHeader(&head);

  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  int recordSize = attrCount * ATTR_SIZE;

  unsigned char* slotPointer = buffer + HEADER_SIZE + slotCount + (recordSize * slotNum);
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}
