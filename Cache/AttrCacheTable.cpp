#include "AttrCacheTable.h"
#include <stdio.h>
#include <cstring>

AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf){
  if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;
  if(attrCache[relId] == nullptr) return E_RELNOTOPEN;

  int count = 0;

  for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next){
    count++;
    if(entry->attrCatEntry.offset == attrOffset){
      *attrCatBuf = entry->attrCatEntry;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS], AttrCatEntry* attrCatEntry){
  strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
  strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
  attrCatEntry->attrType = record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
  attrCatEntry->offset = record[ATTRCAT_OFFSET_INDEX].nVal;
  // printf("setting offset to %f\n", record[ATTRCAT_OFFSET_INDEX].nVal);
  attrCatEntry->rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
}
