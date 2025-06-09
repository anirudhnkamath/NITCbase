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

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute newRelationName;
  strcpy(newRelationName.sVal, newName);
  
  RecId relExists = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, newRelationName, EQ);
  if(relExists.block != -1 || relExists.slot != -1)
    return E_RELEXIST;

  Attribute oldRelationName;
  strcpy(oldRelationName.sVal, oldName);

  // change relname in relation catalog
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  RecId relcatId = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, oldRelationName, EQ);
  if(relcatId.slot == -1 && relcatId.block == -1)
    return E_RELNOTEXIST;

  RecBuffer relcatBuffer(RELCAT_BLOCK);
  Attribute relcatRecord[RELCAT_NO_ATTRS];
  relcatBuffer.getRecord(relcatRecord, relcatId.slot);
  strcpy(relcatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);
  relcatBuffer.setRecord(relcatRecord, relcatId.slot);

  // updating entries in attribute catalog
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  int numAttrs = relcatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

  for(int i=0; i<numAttrs; i++) {
    RecId attrcatId = linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);
    RecBuffer attrcatBuffer(attrcatId.block);
    Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
    attrcatBuffer.getRecord(attrcatRecord, attrcatId.slot);
    strcpy(attrcatRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);
    attrcatBuffer.setRecord(attrcatRecord, attrcatId.slot);
  }
  return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute relNameAttr;
  strcpy(relNameAttr.sVal, relName);
  RecId relExists = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);
  if(relExists.block == -1 && relExists.slot == -1)
    return E_RELNOTEXIST;

  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  RecId attrToRenameRecId{-1, -1};
  Attribute attrcatRecord[ATTRCAT_NO_ATTRS];

  while(true) {
    RecId curRecId = linearSearch(ATTRCAT_RELID, (char*)(char*)(char*)(char*)(char*)(char*)(char*)(char*)(char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
    if(curRecId.block == -1 && curRecId.slot == -1)
      break;

    RecBuffer recBuffer(curRecId.block);
    recBuffer.getRecord(attrcatRecord, curRecId.slot);

    if(strcmp(attrcatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
      return E_ATTREXIST;

    if(strcmp(attrcatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0) {
      attrToRenameRecId.block = curRecId.block;
      attrToRenameRecId.slot = curRecId.slot;
    }
  }

  if(attrToRenameRecId.slot == -1 && attrToRenameRecId.block == -1)
    return E_ATTRNOTEXIST;

  Attribute foundRecord[ATTRCAT_NO_ATTRS];
  RecBuffer recBuffer(attrToRenameRecId.block);
  recBuffer.getRecord(foundRecord, attrToRenameRecId.slot);
  strcpy(foundRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
  recBuffer.setRecord(foundRecord, attrToRenameRecId.slot);

  return SUCCESS;
}