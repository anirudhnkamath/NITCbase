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



// inserts all records of srcRel which match a condition into targetRel
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if(srcRelId == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  AttrCatEntry srcAttrCatEntry;
  int attrCatError = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &srcAttrCatEntry);
  if(attrCatError == E_ATTRNOTEXIST)
    return E_ATTRNOTEXIST;

  // checking if relation and given attribute types match
  int type = srcAttrCatEntry.attrType;
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

  RelCatEntry srcRelCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

  // create arguments for the createRel() function
  int srcNumAttrs = srcRelCatEntry.numAttrs;
  char attrNames[srcNumAttrs][ATTR_SIZE];
  int attrTypes[srcNumAttrs];

  // getting required attrnames and attrtypes
  for(int i=0; i<srcNumAttrs; i++) {
    AttrCatEntry curAttrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &curAttrCatEntry);
    strcpy(attrNames[i], curAttrCatEntry.attrName);
    attrTypes[i] = curAttrCatEntry.attrType;
  }

  // create and open targetRel
  int createRelRes = Schema::createRel(targetRel, srcNumAttrs, attrNames, attrTypes);
  if(createRelRes != SUCCESS)
    return createRelRes;

  int openTargetRelRes = Schema::openRel(targetRel);
  if(openTargetRelRes != SUCCESS) {
    Schema::deleteRel(targetRel);
    return openTargetRelRes;
  }
  int targetRelId = OpenRelTable::getRelId(targetRel);

  // iterate through srcrel and insert required records to targetrel
  Attribute curRecord[srcNumAttrs];
  RelCacheTable::resetSearchIndex(srcRelId);
  AttrCacheTable::resetSearchIndex(srcRelId, attr);
  while(BlockAccess::search(srcRelId, curRecord, attr, attrVal, op) == SUCCESS) {
    int ret = BlockAccess::insert(targetRelId, curRecord);

    // if fail, close and delete targetrel
    if(ret != SUCCESS) {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }

  Schema::closeRel(targetRel);
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

// copy all records srcrel to new relation targetrel
int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if(srcRelId < 0)
    return E_RELNOTOPEN;
  
  RelCatEntry srcRelCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
  int numAttrs = srcRelCatEntry.numAttrs;

  // get the attrnames and attrtypes to create new table
  char attrNames[numAttrs][ATTR_SIZE];
  int attrTypes[numAttrs];
  for(int i=0; i<numAttrs; i++) {
    AttrCatEntry curAttrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &curAttrCatEntry);
    strcpy(attrNames[i], curAttrCatEntry.attrName);
    attrTypes[i] = curAttrCatEntry.attrType;
  }

  // create and open table
  int createRelRes = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
  if(createRelRes != SUCCESS)
    return createRelRes;

  int openTargetRelRes = Schema::openRel(targetRel);
  if(openTargetRelRes != SUCCESS) {
    Schema::deleteRel(targetRel);
    return openTargetRelRes;
  }
  int targetRelId = OpenRelTable::getRelId(targetRel);

  // through srcrel and insert required records to targetrel
  Attribute curRecord[numAttrs];
  RelCacheTable::resetSearchIndex(srcRelId);
  while(BlockAccess::project(srcRelId, curRecord) == SUCCESS) {
    int ret = BlockAccess::insert(targetRelId, curRecord);

    // if fail, close and delete targetrel
    if(ret != SUCCESS) {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }

  Schema::closeRel(targetRel);
  return SUCCESS;
}

// copy all records from ONLY requested attributes to new relation
int Algebra::project(char srcRel[ATTR_SIZE], char tarRel[ATTR_SIZE], int tarNumAttrs, char tarAttrs[][ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if(srcRelId < 0)
    return E_RELNOTOPEN;

  RelCatEntry srcRelCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
  int srcNumAttrs = srcRelCatEntry.numAttrs;

  // gets the offsets and types of targetrel from data present in attrcache of srcrel
  int attrOffsets[tarNumAttrs];
  int attrTypes[tarNumAttrs];
  for(int i=0; i<tarNumAttrs; i++) {
    AttrCatEntry attrCatEntry;
    int attrRes = AttrCacheTable::getAttrCatEntry(srcRelId, tarAttrs[i], &attrCatEntry);
    if(attrRes != SUCCESS)
      return attrRes;

    attrOffsets[i] = attrCatEntry.offset;
    attrTypes[i] = attrCatEntry.attrType;
  }

  // create relation
  int createTargetRes = Schema::createRel(tarRel, tarNumAttrs, tarAttrs, attrTypes);
  if(createTargetRes != SUCCESS)
    return createTargetRes;


  // open relation
  int openTargetRes = OpenRelTable::openRel(tarRel);
  if(openTargetRes < 0) {
    Schema::deleteRel(tarRel);
    return openTargetRes;
  }
  int tarRelId = OpenRelTable::getRelId(tarRel);

  // iterate through all records of srcrel
  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute srcRecord[srcNumAttrs];
  while(BlockAccess::project(srcRelId, srcRecord) == SUCCESS) {

    // filter the records
    Attribute tarRecord[tarNumAttrs];
    for(int i=0; i<tarNumAttrs; i++)
      tarRecord[i] = srcRecord[attrOffsets[i]];
    
    // insert filtered records to targetrel
    int insertRes = BlockAccess::insert(tarRelId, tarRecord);
    if(insertRes != SUCCESS) {
      Schema::closeRel(tarRel);
      Schema::deleteRel(tarRel);
      return insertRes;
    }

  }

  // close targetrel and return
  Schema::closeRel(tarRel);
  return SUCCESS;
}