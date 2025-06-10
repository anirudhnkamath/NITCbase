#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>
#include <stdio.h>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {
  for(int i=0; i<MAX_OPEN; i++) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
  }

  // SETTING UP RELCACHE ENTRIES
  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  struct RelCacheEntry relCacheEntry;
  
  // Relation catalog
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);  
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry.relCatEntry));
  
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[RELCAT_RELID] = relCacheEntry;

  // Attribute catalog
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry.relCatEntry));

  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[ATTRCAT_RELID] = relCacheEntry;

  // SETTING UP ATTRCACHE ENTRIES
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  struct AttrCacheEntry* prevAttrCacheEntry;
  struct AttrCacheEntry* headAttrCacheEntry;

  // Relation catalog
  prevAttrCacheEntry = nullptr;
  headAttrCacheEntry = nullptr;
  for(int i=0; i<6; i++) {
    attrCatBuffer.getRecord(attrCatRecord, i);
    struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));

    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    if(headAttrCacheEntry == nullptr)
      headAttrCacheEntry = attrCacheEntry;
    if(prevAttrCacheEntry != nullptr)
      prevAttrCacheEntry->next = attrCacheEntry;

    prevAttrCacheEntry = attrCacheEntry;
  }
  AttrCacheTable::attrCache[RELCAT_RELID] = headAttrCacheEntry;

  // Attribute catalog
  prevAttrCacheEntry = nullptr;
  headAttrCacheEntry = nullptr;
  for(int i=6; i<12; i++) {
    attrCatBuffer.getRecord(attrCatRecord, i);
    struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));

    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    if(headAttrCacheEntry == nullptr)
      headAttrCacheEntry = attrCacheEntry;

    if(prevAttrCacheEntry)
      prevAttrCacheEntry->next = attrCacheEntry;

    prevAttrCacheEntry = attrCacheEntry;
  }
  AttrCacheTable::attrCache[ATTRCAT_RELID] = headAttrCacheEntry;
  
  // SETTING UP TABLEMETAINFO
  tableMetaInfo[RELCAT_RELID].free = false;
  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
  int existingId = getRelId(relName);
  if(existingId != E_RELNOTOPEN)
    return existingId;

  int relId = getFreeOpenRelTableEntry();
  if(relId == E_CACHEFULL)
    return E_CACHEFULL;

  /****** Setting up Relation Cache entry for the relation ******/

  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute attrVal;
  strcpy(attrVal.sVal, relName);
  RecId relcatId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, attrVal, EQ);

  if(relcatId.block == -1 && relcatId.slot == -1)
    return E_RELNOTEXIST;
    
  RecBuffer recBuffer(relcatId.block);
  Attribute record[RELCAT_NO_ATTRS];
  RelCacheEntry relCacheEntry;
  recBuffer.getRecord(record, relcatId.slot);
  RelCacheTable::recordToRelCatEntry(record, &(relCacheEntry.relCatEntry));
  relCacheEntry.recId.block = relcatId.block;
  relCacheEntry.recId.slot = relcatId.slot;

  RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[relId] = relCacheEntry;

  /****** Setting up Attribute Cache entry for the relation ******/

  AttrCacheEntry* listHead = nullptr;
  AttrCacheEntry* listPrev = nullptr;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  while(true) {
    RecId searchRes = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, attrVal, EQ);

    if(searchRes.block != -1 && searchRes.slot != -1) {
      RecBuffer recBuffer(searchRes.block);
      Attribute record[ATTRCAT_NO_ATTRS];
      AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));

      recBuffer.getRecord(record, searchRes.slot);
      AttrCacheTable::recordToAttrCatEntry(record, &(attrCacheEntry->attrCatEntry));
      attrCacheEntry->recId.block = searchRes.block;
      attrCacheEntry->recId.slot = searchRes.slot;
      attrCacheEntry->next = nullptr;

      if(listHead == nullptr)
        listHead = attrCacheEntry;
      if(listPrev != nullptr)
        listPrev->next = attrCacheEntry;

      listPrev = attrCacheEntry;
    }
    else break;
  }
  AttrCacheTable::attrCache[relId] = listHead;

  /****** Setting up metadata in the Open Relation Table for the relation******/

  tableMetaInfo[relId].free = false;
  strcpy(tableMetaInfo[relId].relName, relName);

  return relId;
}

int OpenRelTable::closeRel(int relId) {
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) 
    return E_NOTPERMITTED;

  if (relId < 0 || relId >= MAX_OPEN)
    return E_OUTOFBOUND;

  if (tableMetaInfo[relId].free)
    return E_RELNOTOPEN;
  
  if(RelCacheTable::relCache[relId]->dirty) {
    union Attribute record[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), record);

    union Attribute attrVal;
    strcpy(attrVal.sVal, record[RELCAT_REL_NAME_INDEX].sVal);
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, attrVal, EQ);

    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(record, recId.slot);
  }
  
  free(RelCacheTable::relCache[relId]);
  RelCacheTable::relCache[relId] = nullptr;

  AttrCacheEntry* curAttrCacheEntry = AttrCacheTable::attrCache[relId];
  while(curAttrCacheEntry) {
    AttrCacheEntry* nextAttrCacheEntry = curAttrCacheEntry->next;
    free(curAttrCacheEntry);
    curAttrCacheEntry = nextAttrCacheEntry;
  }
  AttrCacheTable::attrCache[relId] = nullptr;

  tableMetaInfo[relId].free = true;
  return SUCCESS;
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for(int i=0; i<MAX_OPEN; i++)
    if(tableMetaInfo[i].free == false && strcmp(tableMetaInfo[i].relName, relName) == 0)
      return i;

  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {
  for(int i=0; i<MAX_OPEN; i++)
    if(tableMetaInfo[i].free)
      return i;
  
  return E_CACHEFULL;
}

OpenRelTable::~OpenRelTable() {
  // close all user relations
  for (int i=2; i<MAX_OPEN; i++) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i);
    }
  }
  
  // free relcat and attrcat
  for(int i=0; i<=1; i++) {
    free(RelCacheTable::relCache[i]);
    AttrCacheEntry* prevAttrCacheEntry = nullptr;
    AttrCacheEntry* curAttrCacheEntry = AttrCacheTable::attrCache[i];

    while(curAttrCacheEntry) {
      prevAttrCacheEntry = curAttrCacheEntry;
      curAttrCacheEntry = curAttrCacheEntry->next;
      free(prevAttrCacheEntry);
    }
  }
}

