/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

// Advance clock to next frame in the buffer pool
void BufMgr::advanceClock()
{
    clockHand = (clockHand+1) % numBufs;    
}

void BufMgr::allocBuf(FrameId & frame) 
{
    bool pageFound = false;
    std::uint32_t numPinned = 0;
   
    //try {
        while(!pageFound) {
	    // Advance the clock to next frame
	    advanceClock();
	    
       	    // check if valid is set
       	    if(bufDescTable[clockHand].valid) {
       	        // if valid check refbit and see if was recently referenced.
       	        if(bufDescTable[clockHand].refbit) {
       	            // Reset refbit
       	            bufDescTable[clockHand].refbit = 0;
       	            continue;
       	        }
       	        else {
                    // check if page is pinned
            	    if(bufDescTable[clockHand].pinCnt > 0) {
            	        numPinned++;
             	        continue;
             	    }
             	    else {
             	        // Check if dirty bit is set
             	        if(bufDescTable[clockHand].dirty) {
             	            //FlushFile(File * fileptr);
             	            //bufDescTable[clockHand].Set(File * fileptr, PageID pageNum);
             	            pageFound = true;
             	        }
             	        else {
       	              	    //bufDescTable[clockHand].Set(File * fileptr, PageID pageNum);
    	                    pageFound = true;
    	                } 
    	            }
    	        }
    	    }
    	    // Valid not set
    	    else {
    	        //bufDescTable[clockHand].Set(File * filePtr, PageID pageNum);   
    	        pageFound = true;
    	    }
    	} // end while 
    //if(numPinned >= numBufs) {
    //    throw buffer_exceeded_exception("All buffer frames pinned!");
    //}
    //} catch(buffer_exceeded_exception e) {
    
    //}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  // First check whether the page is already in the buffer pool
  // are we suppose to get frameNo from clockHand index?
  FrameId tmp = bufDescTable[clockHand].frameNo;
  try{
   hashTable->lookup(file, pageNo, tmp);
  }

  catch(HashNotFoundException e){
  // Case 1: If page is not in the buffer pool
  //Allocate buffer frame
  BufMgr::allocBuf(tmp);

  //Read page 
  file->readPage(pageNo);

  //Insert page into hashtab;e
  hashTable->insert(file,pageNo,tmp);

  // invoke Set() on the frame to set it up properly
  bufDescTable[clockHand].Set(file,pageNo);
  // Return a pointer to the frame containing the page via page param
  page = &bufPool[clockHand];
  }

  // Case 2: Page is in the buffer pool
  // Set the appropriate refbit
  bufDescTable[clockHand].refbit = true;
  //Increment pin count for the page
  bufDescTable[clockHand].pinCnt++;
  // return pointer to the frame containing the page via page param
  page = &bufPool[clockHand];
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
  //Is this necessary?
  FrameId tmp = bufDescTable[clockHand].frameNo;

  try{
   //Lookup file and page number
   hashTable->lookup(file, pageNo, bufDescTable[clockHand].frameNo);
   //Decrement pin count

   if(bufDescTable[clockHand].pinCnt == 0){
     throw PageNotPinnedException("PinCnt already 0",pageNo,tmp);
   }

   bufDescTable[clockHand].pinCnt--; //do we need to lookup? Are we decrementing the right pinCnt?

   if(bufDescTable[clockHand].dirty == true){
    //are we checking if the param dirty == false or if bufDesc.dirty == false?
    //Which dirty are we updating and to what value?
    bufDescTable[clockHand].dirty = false;
   }
  }

  catch(PageNotPinnedException e){}
  catch(HashNotFoundException e){}
}

void BufMgr::flushFile(const File* file) 
{
  //Scan bufPool coorect way to scan bufPool?
  for(unsigned int i = 0; i < numBufs; i++){
  try{
  //checks if page corresponds to file
  if(bufDescTable[i].file == file){
   //Checks if dirty bit is true
   if (bufDescTable[i].dirty == true){
    //Flush page to disk
    //file->writePage(bufPool[0]); how to get this to work?

    //Reset dirty bit
    bufDescTable[i].dirty = false;
   }

    //Remove page from hashtable
    hashTable->remove(file,bufDescTable[i].pageNo);

    //Invoke clear() to clear page frame
    bufDescTable[i].Clear();

  }
  //else{continue;}
 }
  catch(BadBufferException e){}
  catch(PagePinnedException e){}
  }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  //Testing
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
