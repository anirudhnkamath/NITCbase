#include "RelCacheTable.h"
#include <stdio.h>
#include <cstring>

// declares the relation cache
RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];

// gets the relation catalog entry from cache and returns as structure
int RelCacheTable::getRelCatEntry(int relId, RelCatEntry* relCatBuf){
  if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;
  if(relCache[relId] == nullptr) return E_RELNOTOPEN;

  *relCatBuf = relCache[relId]->relCatEntry;
  return SUCCESS;
}

// converts given record to structure
void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS], RelCatEntry* relCatEntry){
  strcpy(relCatEntry->relName, record[RELCAT_REL_NAME_INDEX].sVal);
  relCatEntry->numAttrs = (int)record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
  relCatEntry->numRecs = (int)record[RELCAT_NO_RECORDS_INDEX].nVal;
  relCatEntry->firstBlk = (int)record[RELCAT_FIRST_BLOCK_INDEX].nVal;
  relCatEntry->lastBlk = (int)record[RELCAT_LAST_BLOCK_INDEX].nVal;
  relCatEntry->numSlotsPerBlk = (int)record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
}

// returns the search index of relation
int RelCacheTable::getSearchIndex(int relId, RecId* searchIndex) {
  if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;
  if(relCache[relId] == nullptr) return E_RELNOTOPEN;

  *searchIndex = relCache[relId]->searchIndex;
  return SUCCESS;
}

// sets the search index of a relation
int RelCacheTable::setSearchIndex(int relId, RecId* searchIndex) {
  if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;
  if(relCache[relId] == nullptr) return E_RELNOTOPEN;

  relCache[relId]->searchIndex = *searchIndex;
  return SUCCESS;
}

// resets the search index to {-1, -1}
int RelCacheTable::resetSearchIndex(int relId) {
  RecId reset = {-1, -1};
  setSearchIndex(relId, &reset);
  return SUCCESS;
}

// updates the relation catalog entry and dirties the entry
int RelCacheTable::setRelCatEntry(int relId, RelCatEntry* relCatBuf) {
  if(relId < 0 || relId >= MAX_OPEN)
    return E_OUTOFBOUND;
  if(relCache[relId] == nullptr)
    return E_RELNOTOPEN;

  // set dirty bit as change was made in entry
  relCache[relId]->relCatEntry = *relCatBuf;
  relCache[relId]->dirty = true;
  
  return SUCCESS;
}

// converts rel cat entry structure to record
void RelCacheTable::relCatEntryToRecord(RelCatEntry *relCatEntry, union Attribute record[RELCAT_NO_ATTRS]) {
  strcpy(record[RELCAT_REL_NAME_INDEX].sVal, relCatEntry->relName);
  record[RELCAT_NO_ATTRIBUTES_INDEX].nVal = relCatEntry->numAttrs;
  record[RELCAT_NO_RECORDS_INDEX].nVal = relCatEntry->numRecs;
  record[RELCAT_FIRST_BLOCK_INDEX].nVal = relCatEntry->firstBlk;
  record[RELCAT_LAST_BLOCK_INDEX].nVal = relCatEntry->lastBlk;
  record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = relCatEntry->numSlotsPerBlk;
}