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

/*
 * Function Name: BufMgr
 * Input: uint32
 * Output: BufMgr Object
 * Purpose: Constructor for BufMgr class
 * Creates an array of BufDesc, an array of pages and a BufHashTable
 */
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

/*
 * Function Name: ~BufMgr
 * Input: None
 * Output: None
 * Purpose: Destructor for BufMgr
 * Flushes out all dirty pages from the bufPool
 * then deallocates the buffer pool and the BufDesc Table
 */
BufMgr::~BufMgr() {
  //Goes through bufDescTable and flushes out all pages with a dirty bit
  for (FrameId i = 0; i < numBufs; i++)
  {
    if(bufDescTable[i].dirty == true){
      flushFile(bufDescTable[i].file);
    }
  }
  //Deallocate bufDescTable and bufPool
  delete [] bufDescTable;
  delete [] bufPool;
}

/*
 * Function Name: advanceClock
 * Input: None
 * Output: None
 * Purpose: Advance clock to next frame in the buffer pool
 * Uses modular math to logically make it like a clockHand
 */
void BufMgr::advanceClock()
{
    clockHand = (clockHand+1) % numBufs;
}

/*
 * Function Name: allocBuf
 * Input: FrameId reference
 * Output: None
 * Purpose: Allocates a freem frame using the clock algorithm
 */
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
                            // call writePage from file instead of flushFile
                            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
             	            //FlushFile(bufDescTable[clockHand].file);
             	            // Call clear();
             	            //bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufPool[clockHand]);
             	            pageFound = true;
             	        }
             	        else {
                            // Call clear();
       	              	    //bufDescTable[clockHand].Set(File * fileptr, PageID pageNum);
       	              	    //bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufPool[clockHand].pageNo);
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
    if(numPinned >= numBufs) {
        throw BufferExceededException();
    }
}

/*
 * Function Name: readPage
 * Input: File pointer, constant PageID and reference to a Page
 * Output: None
 * Purpose: Read a page from disk into the buffer pool
 * or set appropriate ref bit and increment pinCnt
 */
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

/*
 * Function Name: unPinPage
 * Input: File pointer, constant PageID and constant bool
 i* Output: None
 * Purpose: Decrement pinCnt of of the input and set the dirty bit
 */
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

   if(dirty == true){
    //are we checking if the param dirty == false or if bufDesc.dirty == false?
    //Which dirty are we updating and to what value?
    bufDescTable[clockHand].dirty = true;
   }
  }

  catch(HashNotFoundException e){}
}

/*
 * Function Name: flushFile
 * Input: File pointer
 * Output: None
 * Purpose:Flushes all pages belonging to the file, remove the pages from the
 * hashTable and clear the corresponding bufDescs
 */
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

/*
 * Function Name: allocPage
 * Input: File pointer, page number and reference to a page
 * Output: None
 * Purpose:
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  //Testing
  //file->alocPage();
  //Call Set()
}

/*
 * Function Name: disposePage
 * Input: File pointer and page number
 * Output: None
 * Purpose: Deletes a page from file.
 * If the page is in the buffer, clear page from buffer and remove
 * from hashTable
 */
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
