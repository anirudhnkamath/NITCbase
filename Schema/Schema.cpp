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
