#include "BlockAccess.h"
#include <stdio.h>
#include <cstring>

// searches a (onty 1) record in a relation based on an operator and updates rec-id of the relation
RecId BlockAccess::linearSearch(int relId, char *attrName, Attribute attrVal, int op) {
  RecId prevRecId = {-1, -1};
  int getSearchIndexRet = RelCacheTable::getSearchIndex(relId, &prevRecId);
  if(getSearchIndexRet != SUCCESS)
    return prevRecId;
  
  int curBlock, curSlot;

  // resets to beginning if prev was {-1, -1}
  if(prevRecId.block == -1 && prevRecId.slot == -1) {
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    curBlock = relCatEntry.firstBlk;
    curSlot = 0;
  }
  // else starts from next slot
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

    // if slot > numSlots, go to next block
    if(curSlot >= curBlockHead.numSlots) {
      curBlock = curBlockHead.rblock;
      curSlot = 0;
      continue;
    }

    // if slot empty, go to next block
    if(curSlotMap[curSlot] == SLOT_UNOCCUPIED) {
      curSlot += 1;
      continue;
    }

    // gets the attribute and attribute type to compare
    Attribute curRecord[curBlockHead.numAttrs];
    curBlockBuffer.getRecord(curRecord, curSlot);
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    Attribute curAttr = curRecord[attrCatEntry.offset];
    int cmpVal = compareAttrs(curAttr, attrVal, attrCatEntry.attrType);

    // if matches, it updates rec-id to current slot and returns
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

  // if no records found, returns {-1, -1} to reset search
  return RecId{-1, -1};
}

// changes name of an existing relation to given name (requires the relation to be closed)
int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute newRelationName;
  strcpy(newRelationName.sVal, newName);
  
  // check if relation with new name already exists
  RecId relExists = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, newRelationName, EQ);
  if(relExists.block != -1 || relExists.slot != -1)
    return E_RELEXIST;

  Attribute oldRelationName;
  strcpy(oldRelationName.sVal, oldName);

  // updates the record in relation catalog corresponding to target relation
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  RecId relcatId = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, oldRelationName, EQ);
  if(relcatId.slot == -1 && relcatId.block == -1)
    return E_RELNOTEXIST;
  
  RecBuffer relcatBuffer(RELCAT_BLOCK);
  Attribute relcatRecord[RELCAT_NO_ATTRS];
  relcatBuffer.getRecord(relcatRecord, relcatId.slot);
  strcpy(relcatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);
  relcatBuffer.setRecord(relcatRecord, relcatId.slot);

  // updates the record in attribute catalog corresponding to target relation
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

// changes name of an existing attribute to given name (requires the relation to be closed)
int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

  // checks if relation actually exists
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute relNameAttr;
  strcpy(relNameAttr.sVal, relName);
  RecId relExists = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);
  if(relExists.block == -1 && relExists.slot == -1)
    return E_RELNOTEXIST;

  // searches records in attribute catalog corresponding given relation's attributes
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  RecId attrToRenameRecId{-1, -1};
  Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
  while(true) {
    RecId curRecId = linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
    if(curRecId.block == -1 && curRecId.slot == -1)
      break;

    RecBuffer recBuffer(curRecId.block);
    recBuffer.getRecord(attrcatRecord, curRecId.slot);

    // if new name exists, return without any changes
    if(strcmp(attrcatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
      return E_ATTREXIST;

    // else find the rec-id to update
    if(strcmp(attrcatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0) {
      attrToRenameRecId.block = curRecId.block;
      attrToRenameRecId.slot = curRecId.slot;
    }
  }

  // if attribute doesnt exists, return
  if(attrToRenameRecId.slot == -1 && attrToRenameRecId.block == -1)
    return E_ATTRNOTEXIST;

  // update the corresponding record in attribute catalog
  Attribute foundRecord[ATTRCAT_NO_ATTRS];
  RecBuffer recBuffer(attrToRenameRecId.block);
  recBuffer.getRecord(foundRecord, attrToRenameRecId.slot);
  strcpy(foundRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
  recBuffer.setRecord(foundRecord, attrToRenameRecId.slot);

  return SUCCESS;
}

// inserts given records to valid relation (requires the relation to be open)
int BlockAccess::insert(int relId, Attribute *record) {
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId, &relCatEntry);

  int curBlockNum = relCatEntry.firstBlk;
  RecId rec_id = {-1, -1};

  int numOfSlots = relCatEntry.numSlotsPerBlk;
  int numOfAttrs = relCatEntry.numAttrs;
  int prevBlockNum = -1;

  // searches all existing relation's block to find free slot
  while(curBlockNum != -1) {
    RecBuffer curRecBuffer(curBlockNum);
    HeadInfo curHead;
    curRecBuffer.getHeader(&curHead);
    unsigned char curSlotMap[numOfSlots];
    curRecBuffer.getSlotMap(curSlotMap);

    for(int i=0; i<numOfSlots; i++) {
      if(curSlotMap[i] == SLOT_UNOCCUPIED) {
        rec_id.block = curBlockNum;
        rec_id.slot = i;
        break;
      }
    }

    // updates rec_id if free slot is found
    if(rec_id.slot != -1 && rec_id.slot != -1)
      break;

    // constantly keeps track of previously searched block
    prevBlockNum = curBlockNum;
    curBlockNum = curHead.rblock;
  }
 
  // no slots are free in existing blocks, new empty block is created
  if(rec_id.block == -1 && rec_id.slot == -1) {
    if(relId == RELCAT_RELID)
      return E_MAXRELATIONS;

    RecBuffer newBlockBuffer;
    int newBlockNum = newBlockBuffer.getBlockNum();
    if(newBlockNum == E_DISKFULL)
      return E_DISKFULL;
    
    // set the rec-id to new block
    rec_id.block = newBlockNum, rec_id.slot = 0;

    // set the header
    HeadInfo newHeader;
    newBlockBuffer.getHeader(&newHeader);
    newHeader.lblock = prevBlockNum;
    newHeader.numSlots = numOfSlots;
    newHeader.numAttrs = numOfAttrs;
    newBlockBuffer.setHeader(&newHeader);

    // set empty slotmap
    unsigned char newBlockSlotMap[numOfSlots];
    for(int k=0; k<numOfSlots; k++)
      newBlockSlotMap[k] = SLOT_UNOCCUPIED;
    newBlockBuffer.setSlotMap(newBlockSlotMap);

    if(prevBlockNum == -1) {
      relCatEntry.firstBlk = newBlockNum;
      RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }
    else {
      HeadInfo prevHead;
      RecBuffer prevBlockBuffer(prevBlockNum);
      prevBlockBuffer.getHeader(&prevHead);
      prevHead.rblock = newBlockNum;
      prevBlockBuffer.setHeader(&prevHead);
    }

    // updates last block of relation as the newly created block
    relCatEntry.lastBlk = newBlockNum;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);
  } 
  
  // inserts new record to the allotted slot
  RecBuffer recBuffer(rec_id.block);
  recBuffer.setRecord(record, rec_id.slot);
  
  // updates slot map
  unsigned char slotMap[numOfSlots];
  recBuffer.getSlotMap(slotMap);
  slotMap[rec_id.slot] = SLOT_OCCUPIED;
  recBuffer.setSlotMap(slotMap);

  // updates numentries (in a block) in header of block
  HeadInfo head;
  recBuffer.getHeader(&head);
  head.numEntries += 1;
  recBuffer.setHeader(&head);

  // updates numrecords in relation cache entry
  relCatEntry.numRecs += 1;
  RelCacheTable::setRelCatEntry(relId, &relCatEntry);
  
  int flag = SUCCESS;
  for(int i=0; i<numOfAttrs; i++) {
    AttrCatEntry curAttrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, i, &curAttrCatEntry);

    if(curAttrCatEntry.rootBlock != -1) {
      int retVal = BPlusTree::bPlusInsert(relId, curAttrCatEntry.attrName, record[curAttrCatEntry.offset], rec_id);
      if(retVal == E_DISKFULL)
        flag = E_INDEX_BLOCKS_RELEASED;
    }
  }
    
  return flag;
}

// searches next matching record (either linear search or bplus search)
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
  RecId foundRecId;

  // check if index exists
  AttrCatEntry reqAttrCatEntry;
  int attrCatEntryRes = AttrCacheTable::getAttrCatEntry(relId, attrName, &reqAttrCatEntry);
  if(attrCatEntryRes != SUCCESS)
    return attrCatEntryRes;

  int rootBlock = reqAttrCatEntry.rootBlock;

  // search
  if(rootBlock == -1) 
    foundRecId = BlockAccess::linearSearch(relId, attrName, attrVal, op);
  else
    foundRecId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
  
  if(foundRecId.block == -1 && foundRecId.slot == -1)
    return E_NOTFOUND;

  // copy the record if found
  RecBuffer foundBlockBuffer(foundRecId.block);
  foundBlockBuffer.getRecord(record, foundRecId.slot);

  return SUCCESS;
}

// to delete a closed valid relation
int BlockAccess::deleteRelation(char* relName) {
  if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

  Attribute relNameAttr;
  strcpy(relNameAttr.sVal, relName);

  // searches in relation catalog for the corresponding record
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  RecId recIdInRelCat = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);
  if(recIdInRelCat.block == -1 && recIdInRelCat.slot == -1)
    return E_RELNOTEXIST;

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  relCatBuffer.getRecord(relCatRecord, recIdInRelCat.slot);

  int numAttrs = relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
  int numSlotsPerBlock = relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;

  // deletes all the record blocks belonging to the relation
  int curBlock = relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
  while(curBlock != -1) {
    RecBuffer delBlockBuffer(curBlock);
    HeadInfo delBlockHead;
    delBlockBuffer.getHeader(&delBlockHead);
    int rBlock = delBlockHead.rblock;
    delBlockBuffer.releaseBlock();
    curBlock = rBlock;
  }


  int numOfAttrsDeleted = 0;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  // iterate through attribute catalog and delete corresponding records
  while(true) {
    RecId recIdInAttrCat = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

    if(recIdInAttrCat.block == -1 && recIdInAttrCat.slot == -1)
      break;

    numOfAttrsDeleted += 1;

    HeadInfo curAttrCatHead;
    RecBuffer curAttrCatBuffer(recIdInAttrCat.block);
    curAttrCatBuffer.getHeader(&curAttrCatHead);

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    curAttrCatBuffer.getRecord(attrCatRecord, recIdInAttrCat.slot);

    int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    
    // update slotmap of block of attribute catalog
    unsigned char slotMap[numSlotsPerBlock];
    curAttrCatBuffer.getSlotMap(slotMap);
    slotMap[recIdInAttrCat.slot] = SLOT_UNOCCUPIED;
    curAttrCatBuffer.setSlotMap(slotMap);
    
    // update header of block of attribute catalog
    curAttrCatHead.numEntries -= 1;
    curAttrCatBuffer.setHeader(&curAttrCatHead);

    // maintain the linked list
    if(curAttrCatHead.numEntries == 0) {
      RecBuffer leftBlock(curAttrCatHead.lblock);
      HeadInfo leftBlockHead;
      leftBlock.getHeader(&leftBlockHead);
      leftBlockHead.rblock = curAttrCatHead.rblock;
      leftBlock.setHeader(&leftBlockHead);

      if(curAttrCatHead.rblock != -1) {
        RecBuffer rightBlock(curAttrCatHead.rblock);
        HeadInfo rightBlockHead;
        rightBlock.getHeader(&rightBlockHead);
        rightBlockHead.lblock = curAttrCatHead.lblock;
        rightBlock.setHeader(&rightBlockHead);
      }
      // if last block of attrcat is changed
      else {
        relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = curAttrCatHead.lblock;
      }

      curAttrCatBuffer.releaseBlock();
    }

    if(rootBlock != -1) {
      BPlusTree::bPlusDestroy(rootBlock);
    }
  }

  // update header of relation catalog
  HeadInfo relCatHead;
  relCatBuffer.getHeader(&relCatHead);
  relCatHead.numEntries -= 1;
  relCatBuffer.setHeader(&relCatHead);

  // update slotmap of relation catalog
  unsigned char relCatSlotMap[SLOTMAP_SIZE_RELCAT_ATTRCAT];
  relCatBuffer.getSlotMap(relCatSlotMap);
  relCatSlotMap[recIdInRelCat.slot] = SLOT_UNOCCUPIED;
  relCatBuffer.setSlotMap(relCatSlotMap);

  // update cache entries corresponding to relcat and attrcat
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
  relCatEntry.numRecs -= 1;
  RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

  RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
  relCatEntry.numRecs -= numOfAttrsDeleted;
  RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);

  return SUCCESS;
}

// returns the next record of the relation based on searchIndex
int BlockAccess::project(int relId, Attribute* record) {
  RecId lastHitRecId;
  RelCacheTable::getSearchIndex(relId, &lastHitRecId);

  // set curblock and curslot based on search index
  int curBlock, curSlot;
  if(lastHitRecId.block == -1 && lastHitRecId.slot == -1) {
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    curBlock = relCatEntry.firstBlk;
    curSlot = 0;
  }
  else {
    curBlock = lastHitRecId.block;
    curSlot = lastHitRecId.slot + 1;
  }

  // iterate through all blocks
  while(curBlock != -1) {
    RecBuffer curBlockBuffer(curBlock);
    HeadInfo curBlockHead;
    curBlockBuffer.getHeader(&curBlockHead);
    unsigned char curBlockSlotMap[curBlockHead.numSlots];
    curBlockBuffer.getSlotMap(curBlockSlotMap);
    
    if(curSlot >= curBlockHead.numSlots)
      curBlock = curBlockHead.rblock, curSlot = 0;
    // break if any record found
    else if(curBlockSlotMap[curSlot] == SLOT_OCCUPIED)
      break;
    else if(curBlockSlotMap[curSlot] = SLOT_UNOCCUPIED)
      curSlot += 1;

  }

  if(curBlock == -1)
    return E_NOTFOUND;

  RecId curHitRecId = {curBlock, curSlot};
  RelCacheTable::setSearchIndex(relId, &curHitRecId);

  // copy the record to the record buffer
  RecBuffer foundBlockBuffer(curBlock);
  foundBlockBuffer.getRecord(record, curSlot);
  
  return SUCCESS;
}

