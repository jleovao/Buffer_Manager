/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Authors: Anthony Quintero-Quiroga & Joseph Leovao
 * PID: A10845368 & A
 * Date: 04/26/2017
 * Filename: buffer.cpp
 * Purpose of File: Defines functions described in buffer.h
 * This is where the majority of our buffer manager is defined.
 * This file contains functions for constructing a buffMgr, destructing a buffMgr,
 * allocating a buffer frame using the clock algorithm, flushing dirty files and
 * other page operations
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
 * Creates an array of BufDesc, an array of pages, a BufHashTable
 * and initializes a clockHand.
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
  //Deallocate bufDescTable, bufPool and hashTable
  delete [] bufDescTable;
  delete [] bufPool;
  //delete [] hashTable;
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
 bool frameFound = false;
 std::uint32_t numPinned = 0;

 // Will continue through the buffPool until an appropriate frame is found
 while(frameFound == false) {
 // Advance the clock to next frame
 advanceClock();

 // Throws a BufferExceededException if all pages are pinned
 if(numPinned >= numBufs) {
   throw BufferExceededException();
 }

 // check if valid is set
 if(bufDescTable[clockHand].valid == true) {
  // if valid check refbit and see if was recently referenced.
  if(bufDescTable[clockHand].refbit == true) {
  // Reset refbit
  bufDescTable[clockHand].refbit = false;
  continue;
  }

  // corresponds to 2nd if statement if refbit is not set
  else {
  // check if page is pinned. reloop if it is
   if(bufDescTable[clockHand].pinCnt > 0) {
    numPinned++;
    continue;
   }

   // if page is not pinned
   else {
   // Allocated buffer frame has a valid page,
   // Remove entry from hash table
    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);


    // Check if dirty bit is set
    if(bufDescTable[clockHand].dirty) {
    // call writePage to flush file
    bufDescTable[clockHand].file->writePage(bufPool[clockHand]);

    // Call clear() to Set page
    bufDescTable[clockHand].Clear();

    // Frame found so exit while loop
    frameFound = true;
    }


    else {
    // Call clear();
    bufDescTable[clockHand].Clear();
    frameFound = true;
    }

    // Page is valid, set frame
    frame = bufDescTable[clockHand].frameNo;
    }
   }
  }

 // corresponds to first if statement
 // Valid not set
  else {
   bufDescTable[clockHand].Clear();
   //frame = clockHand; //could be correct?
   frame = bufDescTable[clockHand].frameNo;
   frameFound = true;
   }
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
  FrameId tmp;
  try{
   // Case 2: page is in the buffer pool
   hashTable->lookup(file, pageNo, tmp);
   // Set the appropriate refbit
   bufDescTable[tmp].refbit = true;
   // Increment pin count for the page
   bufDescTable[tmp].pinCnt++;
   // return pointer to the frame containing the page via page param
   page = &bufPool[tmp];
  }

  catch(HashNotFoundException e){
      // Case 1: If page is not in the buffer pool
      //Allocate buffer frame
      allocBuf(tmp);

      //Read page 
      bufPool[tmp] = file->readPage(pageNo);

      //Insert page into hashtable
      hashTable->insert(file,pageNo,tmp);

      // invoke Set() on the frame to set it up properly
      bufDescTable[tmp].Set(file,pageNo);
      // Return a pointer to the frame containing the page via page param
      page = &bufPool[tmp];
  }
}

/*
 * Function Name: unPinPage
 * Input: File pointer, constant PageID and constant bool
 i* Output: None
 * Purpose: Decrement pinCnt of of the input and set the dirty bit
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
  FrameId tmp;

  try{
   //Lookup file and page number
   hashTable->lookup(file, pageNo, tmp);

   // Throw exception if pinCnt is already 0
   if(bufDescTable[tmp].pinCnt == 0){
     throw PageNotPinnedException("PinCnt already 0",pageNo,tmp);
   }

   // Decrement pin count
   bufDescTable[tmp].pinCnt--;

   // This check is for the test cases
   if(dirty == true){
    bufDescTable[tmp].dirty = true;
   }
  }

  // Catch exception if lookup failed
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
  // Scan bufPool
  for(unsigned int i = 0; i < numBufs; i++){
  // checks if page corresponds to file and is valid
  if(bufDescTable[i].file == file && bufDescTable[i].valid == true){
   // Throws exception if page already pinned
   if(bufDescTable[i].pinCnt > 0) {
       throw PagePinnedException("Page is pinned", bufDescTable[i].pageNo, bufDescTable[i].frameNo);
   }

   // Check if dirty bit is true
   if (bufDescTable[i].dirty == true){
    //Flush page to disk
    bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);

    //Reset dirty bit
    bufDescTable[i].dirty = false;
   }

    //Remove page from hashtable
    hashTable->remove(file,bufDescTable[i].pageNo);

    //Invoke clear() to clear page frame
    bufDescTable[i].Clear();

  }
  else{
      throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
  }
 }
}

/*
 * Function Name: allocPage
 * Input: File pointer, page number and reference to a page
 * Output: None
 * Purpose: Allocates an empty page and returns both the page number of 
 *          the newly allocated page to the caller via the pageNo param
 *          and a pointer to the buffer frame allocated for the page via
 *          page param
 */

// InvalidRecordException thrown during main
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  FrameId frameNo;
  // Allocate an empty page in the specified file which returns a newly allocated page
  Page currentPage = file->allocatePage();
  // Obtain a buffer pool frame
  allocBuf(frameNo);
  bufPool[frameNo] = currentPage;
  // Entry is inserted into the hash table
  hashTable->insert(file, currentPage.page_number(), frameNo);
  //Call Set() on the frame
  bufDescTable[frameNo].Set(file, currentPage.page_number());
  // return both page number of newly allocated page to the caller via the pageNo param
  // and a pointer to the buffer frame allocated for the page via page param
  pageNo = currentPage.page_number();
  page = &bufPool[frameNo];
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
    FrameId tmp;
    // This method deletes a particular page from file.
    try {
        hashTable->lookup(file, PageNo, tmp);
        // Make sure that if the page to be deleted is allocated to a frame in the buffer
        // pool, that frame is freed and correspondingly entry from hash table is also
        // removed
        bufDescTable[tmp].Clear();
        hashTable->remove(file, PageNo);
    } catch(HashNotFoundException e) {}

    // After checks, delete page from the file
    file->deletePage(PageNo);
}

/*
 * Function Name: printSelf
 * Input: void
 * Output: void
 * Purpose: Prints out the total number of valid frames in bufDescTable
 * Iterates through bufDescTable and counts the number of frames whose valid
 * bit is set to true. Then it prints that counted value.
 */
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
