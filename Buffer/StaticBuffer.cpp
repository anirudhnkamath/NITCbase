#include "StaticBuffer.h"
#include <stdio.h>

// initialise the buffer for blocks, metainfo and buffer for BMAP
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

// initialises static buffer
StaticBuffer::StaticBuffer(){

  // reads bmap values from disk and stores in buffer
  for(int i=0; i<4; i++) {
    int offset = BLOCK_SIZE * i;
    Disk::readBlock(blockAllocMap + offset, i);
  }

  //initialises all blocks buffers as empty
  for(int i=0; i<BUFFER_CAPACITY; i++) {
    metainfo[i].free = true;
    metainfo[i].blockNum = -1;
    metainfo[i].dirty = false;
    metainfo[i].timeStamp = -1;
  } 
}

// allocates a free buffer. if not found, replaces and writes back the replaced block
int StaticBuffer::getFreeBuffer(int blockNum){
  if(blockNum < 0 || blockNum > DISK_BLOCKS) 
    return E_OUTOFBOUND;
  
  // increments timestamp of all blocks other than given block
  for(int i=0; i<BUFFER_CAPACITY; i++)
    if(!metainfo[i].free && metainfo[i].blockNum!=blockNum)
      metainfo[i].timeStamp += 1;

  // find free block if exists
  int allocatedBuffer = -1;
  for(int i=0; i<BUFFER_CAPACITY; i++){
    if(metainfo[i].free){
      allocatedBuffer = i;
      break;
    }
  }

  // if no free block exists, uses LRU to replace and writes back if dirty
  if(allocatedBuffer == -1) {

    // finds max timestamp block
    int maxIndex = 0;
    for(int i=0; i<BUFFER_CAPACITY; i++)
      if(metainfo[i].timeStamp > metainfo[maxIndex].timeStamp)
        maxIndex = i;

    // write back
    if(metainfo[maxIndex].dirty)
      Disk::writeBlock(blocks[maxIndex], metainfo[maxIndex].blockNum);
    
    allocatedBuffer = maxIndex;
  }

  // sets up metainfo for allocated block
  metainfo[allocatedBuffer].blockNum = blockNum;
  metainfo[allocatedBuffer].dirty = false;
  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].timeStamp = 0;

  return allocatedBuffer;
}

// find the index of a given block in the static buffer
int StaticBuffer::getBufferNum(int blockNum){
  if(blockNum < 0 || blockNum > DISK_BLOCKS) 
    return E_OUTOFBOUND;

  for(int i=0; i<BUFFER_CAPACITY; i++) 
    if(metainfo[i].blockNum == blockNum) 
      return i;

  return E_BLOCKNOTINBUFFER;
}

// sets the dirty bit of given block
int StaticBuffer::setDirtyBit(int blockNum) {
  int index = getBufferNum(blockNum);

  if(index == E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;
  if(index == E_OUTOFBOUND)
    return E_OUTOFBOUND;

  metainfo[index].dirty = true;
  return SUCCESS;
}

// returns the type of given block from BMAP
int StaticBuffer::getStaticBlockType(int blockNum){
  if(blockNum < 0 || blockNum >= DISK_BLOCKS)
    return E_OUTOFBOUND;

  return blockAllocMap[blockNum];
}

// writes back all the dirty buffers and BMAP
StaticBuffer::~StaticBuffer(){
  // writes back the BMAP to disk
  for(int i=0; i<4; i++) {
    int offset = BLOCK_SIZE * i;
    Disk::writeBlock(blockAllocMap + offset, i); 
  }

  // writes back the dirty blocks only
  for(int i=0; i<BUFFER_CAPACITY; i++)
    if(!metainfo[i].free && metainfo[i].dirty)
      Disk::writeBlock(blocks[i], metainfo[i].blockNum);
}