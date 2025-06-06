#include "OpenRelTable.h"
#include <stdlib.h>
#include <stdio.h>
#include <cstring>

OpenRelTable::OpenRelTable() {
  for(int i=0; i<MAX_OPEN; i++) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }

  // SETTING UP RELCACHE ENTRIES

  // setting up entry of relation catalog (present in relation catalog) in relCache table

  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry.relCatEntry));
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[RELCAT_RELID] = relCacheEntry;

  // setting up entry of attribute catalog (present in relation catalog) in relCache table

  // RecBuffer relCatBlock(RELCAT_BLOCK);           (no need, as we use the same one used earlier)

  // Attribute relCatRecord[RELCAT_NO_ATTRS];       (no need, as we use the same one used earlier)

  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

  // struct RelCacheEntry relCacheEntry;            (no need, as we use the same one used earlier)
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry.relCatEntry));
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[ATTRCAT_RELID] = relCacheEntry;

  // SETTING UP ATTRCACHE ENTRIES

  // setting up attributes of relation catalog (present in attribute catalog) in attrCache table

  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  struct AttrCacheEntry* prevAttrCacheEntry = nullptr;
  struct AttrCacheEntry* headAttrCacheEntry = nullptr;
  for(int i=0; i<6; i++) {
    attrCatBuffer.getRecord(attrCatRecord, i);
    struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));

    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    // set up head pointer
    if(i == 0)
      headAttrCacheEntry = attrCacheEntry;

    if(prevAttrCacheEntry != nullptr)
      prevAttrCacheEntry->next = attrCacheEntry;

    prevAttrCacheEntry = attrCacheEntry;
  }

  AttrCacheTable::attrCache[RELCAT_RELID] = headAttrCacheEntry;

  // setting up attributes of attribute catalog (present in attribute catalog) in attrCache table

  // RecBuffer attrCatBuffer(ATTRCAT_BLOCK);        (no need, as we use the same one used earlier)

  // Attribute attrCatRecord[ATTRCAT_NO_ATTRS];     (no need, as we use the same one used earlier)

  prevAttrCacheEntry = nullptr;                  // (we use the same one used earlier)
  headAttrCacheEntry = nullptr;
  for(int i=6; i<12; i++) {
    attrCatBuffer.getRecord(attrCatRecord, i);
    struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));

    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    // set up head pointer
    if(i == 6)
      headAttrCacheEntry = attrCacheEntry;

    if(prevAttrCacheEntry)
      prevAttrCacheEntry->next = attrCacheEntry;

    prevAttrCacheEntry = attrCacheEntry;
  }

  AttrCacheTable::attrCache[ATTRCAT_RELID] = headAttrCacheEntry;
  
  //SETTING UP TABLES FOR STUDENTS RELATION
  
  // relation cache

  // RecBuffer relCatBlock(RELCAT_BLOCK);           (no need, as we use the same one used earlier)

  // Attribute relCatRecord[RELCAT_NO_ATTRS];       (no need, as we use the same one used earlier)

  relCatBlock.getRecord(relCatRecord, 2);

  // struct RelCacheEntry relCacheEntry;            (no need, as we use the same one used earlier)
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry.relCatEntry));
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = 2;

  RelCacheTable::relCache[2] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
  *RelCacheTable::relCache[2] = relCacheEntry;


  // attribute cache;
  
  prevAttrCacheEntry = nullptr;                  // (we use the same one used earlier)
  headAttrCacheEntry = nullptr;
  for(int i=12; i<16; i++) {
    attrCatBuffer.getRecord(attrCatRecord, i);
    struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));

    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    // set up head pointer
    if(i == 12)
      headAttrCacheEntry = attrCacheEntry;

    if(prevAttrCacheEntry)
      prevAttrCacheEntry->next = attrCacheEntry;

    prevAttrCacheEntry = attrCacheEntry;
  }

  AttrCacheTable::attrCache[2] = headAttrCacheEntry;
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  if(strcmp(relName, RELCAT_RELNAME) == 0)
    return RELCAT_RELID;
  if(strcmp(relName, ATTRCAT_RELNAME) == 0)
    return ATTRCAT_RELID;

  return E_RELNOTOPEN;
}

OpenRelTable::~OpenRelTable() {
  for(int i=0; i<MAX_OPEN; i++) {
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

