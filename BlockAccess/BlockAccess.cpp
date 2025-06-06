#include "BlockAccess.h"
#include <stdio.h>
#include <cstring>

RecId BlockAccess::linearSearch(int relId, char *attrName, Attribute attrVal, int op) {
  RecId prevRecId;
  RelCacheTable::getSearchIndex(relId, &prevRecId);
  
  int curBlock, curSlot;

  if(prevRecId.block == -1 && prevRecId.slot == -1) {
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    curBlock = relCatEntry.firstBlk;
    curSlot = 0;
  }
  else {
    curBlock = prevRecId.block;
    curSlot = prevRecId.slot+1;
  }

  while(curBlock != -1) {
    RecBuffer curBlockBuffer(curBlock);

    HeadInfo curBlockHead;
    curBlockBuffer.getHeader(&curBlockHead);
    unsigned char curSlotMap[curBlockHead.numSlots];
    curBlockBuffer.getSlotMap(curSlotMap);

    if(curSlot >= curBlockHead.numSlots) {
      curBlock = curBlockHead.rblock;
      curSlot = 0;
      continue;
    }

    if(curSlotMap[curSlot] == SLOT_UNOCCUPIED) {
      curSlot += 1;
      continue;
    }

    Attribute curRecord[curBlockHead.numAttrs];
    curBlockBuffer.getRecord(curRecord, curSlot);

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    
    Attribute curAttr = curRecord[attrCatEntry.offset];
    int cmpVal = compareAttrs(curAttr, attrVal, attrCatEntry.attrType);

    if (
      (op == NE && cmpVal != 0) ||
      (op == LT && cmpVal < 0) ||
      (op == LE && cmpVal <= 0) ||
      (op == EQ && cmpVal == 0) ||
      (op == GT && cmpVal > 0) ||
      (op == GE && cmpVal >= 0) 
    ) {
      RecId foundRecId = {curBlock, curSlot};
      RelCacheTable::setSearchIndex(relId, &foundRecId);
      return foundRecId;
    }

    curSlot += 1;
  }

  return RecId{-1, -1};
}