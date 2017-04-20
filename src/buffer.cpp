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
        
    //try{
        // First check whether the page is already in the buffer pool    
        FrameId frameNo;
        hashTable->lookup(file, pageNo, frameNo);
        // Set the appropriate refbit
        bufDescTable[frameNo].refbit = true;
        //Increment pin count for the page
        bufDescTable[frameNo].pinCount++;
        // return pointer to the frame containing the page via page param
        
    //} catch(hash_not_found_exception e) {
        // If page is not in the buffer pool
        // Call allocBuf(frameNo);
        // call file->readPage(pageNo)
        // insert page into hashtable: insert->();
        // invoke Set() on the frame to set it up properly
        // Return a pointer to the frame containing the page via page param
    //}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
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
