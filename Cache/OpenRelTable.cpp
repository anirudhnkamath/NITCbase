#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>
#include <stdio.h>

// declares meta info of cache
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

// initialises the caches and meta info
OpenRelTable::OpenRelTable() {
  // initialise empty cache
  for(int i=0; i<MAX_OPEN; i++) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
  }

  // set up rel cache
  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  struct RelCacheEntry relCacheEntry;
  
  // setting up relcache entry for relation catalog
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);  
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry.relCatEntry));
  
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[RELCAT_RELID] = relCacheEntry;

  // setting up relcache entry for attribute catalog
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry.relCatEntry));

  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[ATTRCAT_RELID] = relCacheEntry;

  // set up attr cache
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  struct AttrCacheEntry* prevAttrCacheEntry;
  struct AttrCacheEntry* headAttrCacheEntry;

  // setting up attr cache entries for rel cat
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

  // setting up attr cache entries for attr cat
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
  
  // updating meta info for slot = 0, and slot = 1;
  tableMetaInfo[RELCAT_RELID].free = false;
  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}

// opens an existing relation
int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
  int existingId = getRelId(relName);
  if(existingId != E_RELNOTOPEN)
    return existingId;

  // gets free slot
  int relId = getFreeOpenRelTableEntry();
  if(relId == E_CACHEFULL)
    return E_CACHEFULL;

  // set up rel cache entry for the relation
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute attrVal;
  strcpy(attrVal.sVal, relName);

  // search relName of relation in relation catalog and get the record
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
  relCacheEntry.dirty = false;

  RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[relId] = relCacheEntry;

  // setting up attrcache entries for the relation
  AttrCacheEntry* listHead = nullptr;
  AttrCacheEntry* listPrev = nullptr;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  
  // linear search through attribute catalog and create attr cache linked list using the records
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
      attrCacheEntry->dirty = false;
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

  // updating metainfo for corresponding relId
  tableMetaInfo[relId].free = false;
  strcpy(tableMetaInfo[relId].relName, relName);

  return relId;
}

// closes an open relation
int OpenRelTable::closeRel(int relId) {
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) 
    return E_NOTPERMITTED;

  if (relId < 0 || relId >= MAX_OPEN)
    return E_OUTOFBOUND;

  if (tableMetaInfo[relId].free)
    return E_RELNOTOPEN;
  
  // writes back to buffer if the relcache entry is dirty
  if(RelCacheTable::relCache[relId]->dirty) {
    // convert to record
    union Attribute record[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), record);

    // linear search the record corresponding to relname in relation catalog
    union Attribute attrVal;
    strcpy(attrVal.sVal, record[RELCAT_REL_NAME_INDEX].sVal);
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, attrVal, EQ);

    // write back
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(record, recId.slot);
  }
  
  // frees the relcache entry
  free(RelCacheTable::relCache[relId]);
  RelCacheTable::relCache[relId] = nullptr;

  // frees attrcache linked list
  AttrCacheEntry* curAttrCacheEntry = AttrCacheTable::attrCache[relId];
  while(curAttrCacheEntry) {
    AttrCacheEntry* nextAttrCacheEntry = curAttrCacheEntry->next;
    free(curAttrCacheEntry);
    curAttrCacheEntry = nextAttrCacheEntry;
  }
  AttrCacheTable::attrCache[relId] = nullptr;

  // updates meta info
  tableMetaInfo[relId].free = true;
  return SUCCESS;
}

// gets relId of a valid relation if its open
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for(int i=0; i<MAX_OPEN; i++)
    if(tableMetaInfo[i].free == false && strcmp(tableMetaInfo[i].relName, relName) == 0)
      return i;

  return E_RELNOTOPEN;
}

// gets a free slot in cache, or else returns E_FULL
int OpenRelTable::getFreeOpenRelTableEntry() {
  for(int i=0; i<MAX_OPEN; i++)
    if(tableMetaInfo[i].free)
      return i;
  
  return E_CACHEFULL;
}

// closes all relations and closes and frees slot=0 and 1 of relcache
OpenRelTable::~OpenRelTable() {
  // closes all user relations
  for (int i=2; i<MAX_OPEN; i++)
    if (!tableMetaInfo[i].free)
      OpenRelTable::closeRel(i);
  
  // write back relcache entry of relation catalog
  if(RelCacheTable::relCache[RELCAT_RELID]->dirty) {
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCacheEntry* relCacheEntry = RelCacheTable::relCache[RELCAT_RELID];
    RelCacheTable::relCatEntryToRecord(&relCacheEntry->relCatEntry, relCatRecord);
    RecBuffer recBuffer(relCacheEntry->recId.block);
    recBuffer.setRecord(relCatRecord, relCacheEntry->recId.slot);
  }

  // write back relcache entry of attr cache
  if(RelCacheTable::relCache[ATTRCAT_RELID]->dirty) {
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCacheEntry* relCacheEntry = RelCacheTable::relCache[ATTRCAT_RELID];
    RelCacheTable::relCatEntryToRecord(&relCacheEntry->relCatEntry, relCatRecord);
    RecBuffer recBuffer(relCacheEntry->recId.block);
    recBuffer.setRecord(relCatRecord, relCacheEntry->recId.slot);
  }

  // frees slot = 0 and slot = 1
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

