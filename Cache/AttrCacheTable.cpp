#include "AttrCacheTable.h"
#include <stdio.h>
#include <cstring>

// declares the attribute cache
AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

// gets the attrcache entry from the linked list, given the offset
int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf){
  if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;
  if(attrCache[relId] == nullptr) return E_RELNOTOPEN;

  // iterate through linkedlist
  for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next){
    if(entry->attrCatEntry.offset == attrOffset){
      *attrCatBuf = entry->attrCatEntry;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

// gets the attrcache entry from the linked list, given the attribute name
int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {
  if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;
  if(attrCache[relId] == nullptr) return E_RELNOTOPEN;

  // iterate through LL
  for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
    if(strcmp(attrName, entry->attrCatEntry.attrName) == 0) {
      *attrCatBuf = entry->attrCatEntry;
      return SUCCESS;
    }
  } 

  return E_ATTRNOTEXIST;
}

// convert a given record to structure RelCatEntry
void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS], AttrCatEntry* attrCatEntry){
  strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
  strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
  attrCatEntry->attrType = record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
  attrCatEntry->offset = record[ATTRCAT_OFFSET_INDEX].nVal;
  attrCatEntry->rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
}
