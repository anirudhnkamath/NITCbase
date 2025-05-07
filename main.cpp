#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <stdio.h>

int main(int argc, char *argv[]) {
  Disk disk_run;
 
  unsigned char buffer[BLOCK_SIZE];
  for(int i=0; i<4; i++){
    printf("\nBMAP Block %d:\n", i);
    Disk::readBlock(buffer, i);
    for(auto x : buffer) printf("%d", x);
    printf("\n");
  }

  return 0; 
}
