#include "Schema.h"
#include <stdio.h>
#include <cmath>
#include <cstring>

// opens relation using openrel function and check if it was opened successfully
int Schema::openRel(char relName[ATTR_SIZE]) {
  int ret = OpenRelTable::openRel(relName);
  if(ret >= 0)
    return SUCCESS;

  return ret;
}

// checks if relation is open and close is valid, then calls closeRel function
int Schema::closeRel(char relName[ATTR_SIZE]) {
  if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

  int relId = OpenRelTable::getRelId(relName);
  
  if(relId == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  
  return OpenRelTable::closeRel(relId);
}

// checks if relation is closed and change is permitted, and then calls renamerelation function
int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
  if(strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;
  if(strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

  if(OpenRelTable::getRelId(oldRelName) != E_RELNOTOPEN)
    return E_RELOPEN;

  return BlockAccess::renameRelation(oldRelName, newRelName);
}

// checks if relation is closed and change is permitted, and then calls renameattribute function
int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName) {
  if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

  if(OpenRelTable::getRelId(relName) != E_RELNOTOPEN)
    return E_RELOPEN;

  return BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
}

// verifies given relation details and then creates relation (makes new entries in catalogs)
int Schema::createRel(char relName[],int nAttrs, char attrs[][ATTR_SIZE], int attrtype[]) {

  // checks if relation already exists
  Attribute relNameAsAttrribute;
  strcpy(relNameAsAttrribute.sVal, relName);
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  RecId searchRes = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAsAttrribute, EQ);
  
  if(searchRes.block != -1 && searchRes.slot != -1)
    return E_RELEXIST;

  // checks for duplicate attribute
  for(int i=0; i<nAttrs; i++)
    for(int j=0; j<i; j++)
      if(strcmp(attrs[i], attrs[j]) == 0)
        return E_DUPLICATEATTR;

  // creates new record in relation catalog for the new relation
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
  relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
  relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
  relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016 / (16 * nAttrs + 1)));

  int relcatInsertRes = BlockAccess::insert(RELCAT_RELID, relCatRecord);
  if(relcatInsertRes != SUCCESS)
    return relcatInsertRes;

  // creates new records in attribute catalog for attributes of the new relation
  for(int i=0; i<nAttrs; i++) {
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
    strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
    attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
    attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;
    attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;

    // if insert fails, deletes the relation (also deletes the relation catalog entry)
    int attrcatInsertRes = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
    if(attrcatInsertRes != SUCCESS) {
      deleteRel(relName);
      return E_DISKFULL;
    }
  }

  return SUCCESS;
}

// vareifies relation name and checks if its open, then calls deleterelation function
int Schema::deleteRel(char *relName) {
  if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

  int relId = OpenRelTable::getRelId(relName);
  if(relId != E_RELNOTOPEN)
    return E_RELOPEN;

  int retVal = BlockAccess::deleteRelation(relName);
  return retVal;
}