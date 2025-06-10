#include "StaticBuffer.h"
#include <stdio.h>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer(){
  for(int i=0; i<4; i++) {
    int offset = BLOCK_SIZE * i;
    Disk::readBlock(blockAllocMap + offset, i);
  }
  for(int i=0; i<BUFFER_CAPACITY; i++) {
    metainfo[i].free = true;
    metainfo[i].blockNum = -1;
    metainfo[i].dirty = false;
    metainfo[i].timeStamp = -1;
  } 
}

int StaticBuffer::getFreeBuffer(int blockNum){
  if(blockNum < 0 || blockNum > DISK_BLOCKS) 
    return E_OUTOFBOUND;
  
  for(int i=0; i<BUFFER_CAPACITY; i++)
    if(!metainfo[i].free && metainfo[i].blockNum!=blockNum)
      metainfo[i].timeStamp += 1;

  int allocatedBuffer = -1;
  for(int i=0; i<BUFFER_CAPACITY; i++){
    if(metainfo[i].free){
      allocatedBuffer = i;
      break;
    }
  }

  if(allocatedBuffer == -1) {
    int maxIndex = 0;
    for(int i=0; i<BUFFER_CAPACITY; i++)
      if(metainfo[i].timeStamp > metainfo[maxIndex].timeStamp)
        maxIndex = i;

    if(metainfo[maxIndex].dirty)
      Disk::writeBlock(blocks[maxIndex], metainfo[maxIndex].blockNum);
    
    allocatedBuffer = maxIndex;
  }

  metainfo[allocatedBuffer].blockNum = blockNum;
  metainfo[allocatedBuffer].dirty = false;
  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].timeStamp = 0;

  return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum){
  if(blockNum < 0 || blockNum > DISK_BLOCKS) 
    return E_OUTOFBOUND;

  for(int i=0; i<BUFFER_CAPACITY; i++) 
    if(metainfo[i].blockNum == blockNum) 
      return i;

  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum) {
  int index = getBufferNum(blockNum);

  if(index == E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;
  if(index == E_OUTOFBOUND)
    return E_OUTOFBOUND;

  metainfo[index].dirty = true;
  return SUCCESS;
}

StaticBuffer::~StaticBuffer(){
  for(int i=0; i<4; i++) {
    int offset = BLOCK_SIZE * i;
    Disk::writeBlock(blockAllocMap + offset, i); 
  }
  for(int i=0; i<BUFFER_CAPACITY; i++)
    if(!metainfo[i].free && metainfo[i].dirty) {
      Disk::writeBlock(blocks[i], metainfo[i].blockNum);
    }
}