#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <stdio.h>

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;
  
  for(int i=0; i<3; i++){
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(i, &relCatEntry);
    printf("Relation: %s\n", relCatEntry.relName);

    for(int j=0; j<relCatEntry.numAttrs; j++){
      AttrCatEntry attrCatEntry;
      AttrCacheTable::getAttrCatEntry(i, j, &attrCatEntry);
      printf("  %s: %s\n", attrCatEntry.attrName, (attrCatEntry.attrType == 0) ? "NUM" : "STR");
    }
  }

  return 0;
}
