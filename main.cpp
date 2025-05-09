#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <stdio.h>

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;

  RecBuffer relCatBuffer(RELCAT_BLOCK);
  HeadInfo relCatHeader;

  relCatBuffer.getHeader(&relCatHeader);

  for(int i=0; i<relCatHeader.numEntries; i++){

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBuffer.getRecord(relCatRecord, i);
    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
 
    int curBlock = ATTRCAT_BLOCK;
    while(curBlock != -1){

      RecBuffer attrCatBuffer(curBlock);
      HeadInfo attrCatHeader;
      attrCatBuffer.getHeader(&attrCatHeader);

      for(int j=0; j<attrCatHeader.numEntries; j++){
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRecord, j);

        if(strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal) == 0){
          const char* attrType = (attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER) ? "NUM" : "STRING";
          printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
        }
      }

      curBlock = attrCatHeader.rblock;
    }

  }

  return 0;
}
