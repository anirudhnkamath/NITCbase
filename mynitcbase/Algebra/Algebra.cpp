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

// equi-joins two relations with unique attributes names except the join attribute
int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]) {
  int srcRelId1 = OpenRelTable::getRelId(srcRelation1);
  int srcRelId2 = OpenRelTable::getRelId(srcRelation2);

  if(srcRelId1 == E_RELNOTOPEN || srcRelId2 == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  AttrCatEntry attrCatEntry1, attrCatEntry2;
  int attrRes1 = AttrCacheTable::getAttrCatEntry(srcRelId1, attribute1, &attrCatEntry1);
  int attrRes2 = AttrCacheTable::getAttrCatEntry(srcRelId2, attribute2, &attrCatEntry2);

  if(attrRes1 != SUCCESS || attrRes2 != SUCCESS)
    return E_ATTRNOTEXIST;

  if(attrCatEntry1.attrType != attrCatEntry2.attrType)
    return E_ATTRTYPEMISMATCH;

  RelCatEntry relCatEntry1, relCatEntry2;
  RelCacheTable::getRelCatEntry(srcRelId1, &relCatEntry1);
  RelCacheTable::getRelCatEntry(srcRelId2, &relCatEntry2);
  int nAttrs1 = relCatEntry1.numAttrs, nAttrs2 = relCatEntry2.numAttrs;

  // checks if duplicate attribute exists in relations other than the join attribute
  // for each attribute in rel1, search its name in rel2, and return if exists
  for(int i=0; i<relCatEntry1.numAttrs; i++) {
    if(i == attrCatEntry1.offset)
      continue;
    
    AttrCatEntry temp1, temp2;
    AttrCacheTable::getAttrCatEntry(srcRelId1, i, &temp1);
    int dupFoundRes = AttrCacheTable::getAttrCatEntry(srcRelId2, temp1.attrName, &temp2);

    if(dupFoundRes == SUCCESS)
      return E_DUPLICATEATTR;
  }

  // create index for join attribute in relation 2
  int indexRes = BPlusTree::bPlusCreate(srcRelId2, attribute2);
  if(indexRes != SUCCESS)
    return indexRes;

  // prepare the entries needed to create new table
  int nAttrsTarget = nAttrs1 + nAttrs2 - 1;
  char targetRelAttrNames[nAttrsTarget][ATTR_SIZE];
  int targetRelAttrTypes[nAttrsTarget];

  int fillIndex = -1;
  // copy table 1 entries to new table entries
  for(int i=0; i<nAttrs1; i++) {
    AttrCatEntry temp;
    AttrCacheTable::getAttrCatEntry(srcRelId1, i, &temp);

    fillIndex += 1;
    strcpy(targetRelAttrNames[fillIndex], temp.attrName);
    targetRelAttrTypes[fillIndex] = temp.attrType;
  }
  // copy table 2 entries to new table entries
  for(int i=0; i<nAttrs2; i++) {
    // skip the join attribute in the second table
    if(i == attrCatEntry1.offset)
      continue;

    AttrCatEntry temp;
    AttrCacheTable::getAttrCatEntry(srcRelId2, i, &temp);

    fillIndex += 1;
    strcpy(targetRelAttrNames[fillIndex], temp.attrName);
    targetRelAttrTypes[fillIndex] = temp.attrType;
  }

  // creates target relation to insert
  int createRes = Schema::createRel(targetRelation, nAttrsTarget, targetRelAttrNames, targetRelAttrTypes);
  if(createRes != SUCCESS)
    return createRes;

  int targetRelId = OpenRelTable::openRel(targetRelation);
  if(targetRelId < 0) {
    Schema::deleteRel(targetRelation);
    return targetRelId;
  }
  
  Attribute record1[nAttrs1];
  Attribute record2[nAttrs2];
  Attribute targetRecord[nAttrsTarget];

  // iterate through all records of rel1 to find matching records in rel2
  RelCacheTable::resetSearchIndex(srcRelId1);
  while(BlockAccess::project(srcRelId1, record1) == SUCCESS) {

    RelCacheTable::resetSearchIndex(srcRelId2);
    AttrCacheTable::resetSearchIndex(srcRelId2, attribute2);

    // iterate through all matching records in rel2
    while(BlockAccess::search(srcRelId2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS) {

      // create the record to insert
      int recordFillIndex = -1;
      for(int i=0; i<nAttrs1; i++) {
        recordFillIndex += 1;
        targetRecord[recordFillIndex] = record1[i];
      }
      for(int i=0; i<nAttrs2; i++) {
        if(i == attrCatEntry2.offset)
          continue;
        else {
          recordFillIndex += 1;
          targetRecord[recordFillIndex] = record2[i];
        }
      }

      // insert
      int insertRes = BlockAccess::insert(targetRelId, targetRecord);
      if(insertRes != SUCCESS) {
        OpenRelTable::closeRel(targetRelId);
        Schema::deleteRel(targetRelation);
      }
    }
  }

  OpenRelTable::closeRel(targetRelId);
  return SUCCESS;
}