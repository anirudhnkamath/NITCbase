#include "Algebra.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>

// utility function, checks if its a valid number 
bool isNumber(char *str) {
  int len;
  float ignore;
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}



// repeatedly calls linear search to select records from relation based on an operator
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if(srcRelId == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  AttrCatEntry attrCatEntry;
  int attrCatError = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
  if(attrCatError == E_ATTRNOTEXIST)
    return E_ATTRNOTEXIST;

  // checking if relation and given attribute types match
  int type = attrCatEntry.attrType;
  Attribute attrVal;
  if(type == NUMBER) {
    if(isNumber(strVal))
      attrVal.nVal = atof(strVal);
    else
      return E_ATTRTYPEMISMATCH;
  }
  else {
    strcpy(attrVal.sVal, strVal);
  }

  RelCacheTable::resetSearchIndex(srcRelId);

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

  printf("|");
  for (int i=0; i<relCatEntry.numAttrs; i++) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
    printf(" %s |", attrCatEntry.attrName);
  }
  printf("\n");

  // calling linear search repeatedly
  while (true) {
    RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

    if (searchRes.block != -1 && searchRes.slot != -1) {
      RecBuffer recBuffer(searchRes.block);

      Attribute curRecord[relCatEntry.numAttrs];
      recBuffer.getRecord(curRecord, searchRes.slot);
      printf("|");
      for (int i=0; i<relCatEntry.numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

        if(attrCatEntry.attrType == NUMBER)
          printf(" %f |", curRecord[i].nVal);
        else
          printf(" %s |", curRecord[i].sVal);
      }
      printf("\n");

    }

    else break;
  }

  return SUCCESS;
}

// verifies the data type of record to insert and then calls insert function
int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {
  if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

  int relId = OpenRelTable::getRelId(relName);
  if(relId == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId, &relCatEntry);

  // checking if number of attrs match
  if(relCatEntry.numAttrs != nAttrs)
    return E_NATTRMISMATCH;

  // checking data types
  union Attribute recordValues[nAttrs];
  for(int i=0; i<nAttrs; i++) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);
    int type = attrCatEntry.attrType;

    if(type == NUMBER) {
      if(isNumber(record[i]))
        recordValues[i].nVal = atof(record[i]);
      else
        return E_ATTRTYPEMISMATCH;
    }
    else if(type == STRING){
      strcpy(recordValues[i].sVal, record[i]);
    }
  }

  int retVal = BlockAccess::insert(relId, recordValues);
  return retVal;
}
