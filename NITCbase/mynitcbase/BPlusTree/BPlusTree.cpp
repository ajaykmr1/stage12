#include "BPlusTree.h"

#include <cstring>
#include "iostream"

static int c;

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    // declare searchIndex which will be used to store search index for attrName.
    IndexId searchIndex;

    /* get the search index corresponding to attribute with name attrName
       using AttrCacheTable::getSearchIndex(). */

    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;

    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare variables block and index which will be used during search
    int block, index;
    RecId recid{-1, -1};

    if (searchIndex.block == -1 && searchIndex.index == -1)
    {
        // (search is done for the first time)

        // start the search from the first entry of root.
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1)
        {
            return recid;
        }
    }
    else
    {
        /*a valid searchIndex points to an entry in the leaf index of the attribute's
        B+ Tree which had previously satisfied the op for the given attrVal.*/

        block = searchIndex.block;
        index = searchIndex.index + 1; // search is resumed from the next index.

        // load block into leaf using IndLeaf::IndLeaf().
        IndLeaf leaf(block);

        // declare leafHead which will be used to hold the header of leaf.
        HeadInfo leafHead;

        // load header into leafHead using BlockBuffer::getHeader().
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries)
        {
            /* (all the entries in the block has been searched; search from the
            beginning of the next leaf index block. */

            // update block to rblock of current block and index to 0.
            block = leafHead.rblock;
            index = 0;

            if (block == -1)
            {
                // (end of linked list reached - the search is done.)
                printf("%d\n", c);

                return recid;
            }
        }
    }

    /******  Traverse through all the internal nodes according to value
             of attrVal and the operator op                             ******/

    /* (This section is only needed when
        - search restarts from the root block (when searchIndex is reset by caller)
        - root is not a leaf
        If there was a valid search index, then we are already at a leaf block
        and the test condition in the following loop will fail)
    */

    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
    { // use StaticBuffer::getStaticBlockType()

        // load the block into internalBlk using IndInternal::IndInternal().
        IndInternal internalBlk(block);

        HeadInfo intHead;

        // load the header of internalBlk into intHead using BlockBuffer::getHeader()
        internalBlk.getHeader(&intHead);

        // declare intEntry which will be used to store an entry of internalBlk.
        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE)
        {
            /*
            - NE: need to search the entire linked list of leaf indices of the B+ Tree,
            starting from the leftmost leaf index. Thus, always move to the left.

            - LT and LE: the attribute values are arranged in ascending order in the
            leaf indices of the B+ Tree. Values that satisfy these conditions, if
            any exist, will always be found in the left-most leaf index. Thus,
            always move to the left.
            */

            // load entry in the first slot of the block into intEntry
            // using IndInternal::getEntry().

            internalBlk.getEntry(&intEntry, index);

            block = intEntry.lChild;
        }
        else
        {
            /*
            - EQ, GT and GE: move to the left child of the first entry that is
            greater than (or equal to) attrVal
            (we are trying to find the first entry that satisfies the condition.
            since the values are in ascending order we move to the left child which
            might contain more entries that satisfy the condition)
            */

            /*
             traverse through all entries of internalBlk and find an entry that
             satisfies the condition.
             if op == EQ or GE, then intEntry.attrVal >= attrVal
             if op == GT, then intEntry.attrVal > attrVal
             Hint: the helper function compareAttrs() can be used for comparing
            */
            int i = 0;
            for (; i < intHead.numEntries; i++)
            {
                internalBlk.getEntry(&intEntry, i);

                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                c++;
                if (
                    (op == EQ && cmpVal == 0) || // if op is "equal to"
                    (op == GT && cmpVal > 0) ||  // if op is "greater than"
                    (op == GE && cmpVal >= 0)    // if op is "greater than or equal to"
                )
                {
                    break;
                }
            }

            if (i < intHead.numEntries)
            {
                // move to the left child of that entry
                block = intEntry.lChild; // left child of the entry
            }
            else
            {
                // move to the right child of the last entry of the block
                // i.e numEntries - 1 th entry of the block
                internalBlk.getEntry(&intEntry, (intHead.numEntries - 1));

                block = intEntry.rChild;
            }
        }
    }

    // NOTE: `block` now has the block number of a leaf index block.

    /******  Identify the first leaf index entry from the current position
                that satisfies our condition (moving right)             ******/

    while (block != -1)
    {
        // load the block into leafBlk using IndLeaf::IndLeaf().
        IndLeaf leafBlk(block);
        HeadInfo leafHead;

        // load the header to leafHead using BlockBuffer::getHeader().
        leafBlk.getHeader(&leafHead);

        // declare leafEntry which will be used to store an entry from leafBlk
        Index leafEntry;

        while (index < leafHead.numEntries)
        {

            // load entry corresponding to block and index into leafEntry
            // using IndLeaf::getEntry().
            leafBlk.getEntry(&leafEntry, index);

            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);
            c++;
            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0))
            {
                // (entry satisfying the condition found)
                searchIndex.block = block;
                searchIndex.index = index;

                // set search index to {block, index}
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);

                // return the recId {leafEntry.block, leafEntry.slot}.
                recid.block = leafEntry.block;
                recid.slot = leafEntry.slot;
                return recid;
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0)
            {
                /*future entries will not satisfy EQ, LE, LT since the values
                    are arranged in ascending order in the leaves */
                //printf("%d\n", c);

                return recid;
            }

            // search next index.
            ++index;
        }

        /*only for NE operation do we have to check the entire linked list;
        for all the other op it is guaranteed that the block being searched
        will have an entry, if it exists, satisying that op. */
        if (op != NE)
        {
            break;
        }

        // block = next block in the linked list, i.e., the rblock in leafHead.
        // update index to 0.
        block = leafHead.rblock;
        index = 0;
    }
    //printf("%d\n", c);
    return recid;
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE])
{

    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    AttrCatEntry attrCatBuf;
    int retVal = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    // if getAttrCatEntry fails
    //     return the error code from getAttrCatEntry
    if (retVal != SUCCESS)
        return retVal;

    if (attrCatBuf.rootBlock != -1)
    {
        return SUCCESS;
    }

    /******Creating a new B+ Tree ******/

    // get a free leaf block using constructor 1 to allocate a new block
    IndLeaf rootBlockBuf;

    // (if the block could not be allocated, the appropriate error code
    //  will be stored in the blockNum member field of the object)

    // declare rootBlock to store the blockNumber of the new leaf block
    int rootBlock = rootBlockBuf.getBlockNum();

    // if there is no more disk space for creating an index
    if (rootBlock == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    attrCatBuf.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    RelCatEntry relCatEntry;

    // load the relation catalog entry into relCatEntry
    // using RelCacheTable::getRelCatEntry().
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = relCatEntry.firstBlk;

    /***** Traverse all the blocks in the relation and insert them one
           by one into the B+ Tree *****/
    while (block != -1)
    {

        // declare a RecBuffer object for `block` (using appropriate constructor)
        RecBuffer currentbuffer(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];

        // load the slot map into slotMap using RecBuffer::getSlotMap().
        currentbuffer.getSlotMap(slotMap);

        for (int slot = 0; slot < relCatEntry.numSlotsPerBlk; slot++)
        {
            if (slotMap[slot] == SLOT_OCCUPIED)
            {
                Attribute record[relCatEntry.numAttrs];
                currentbuffer.getRecord(record, slot);

                // declare recId and store the rec-id of this record in it
                RecId recId{block, slot};

                // insert the attribute value corresponding to attrName from the record
                // into the B+ tree using bPlusInsert.

                retVal = bPlusInsert(relId, attrName, record[attrCatBuf.offset], recId);

                if (retVal == E_DISKFULL)
                {
                    return E_DISKFULL;
                }
            }
        }

        // get the header of the block using BlockBuffer::getHeader()
        HeadInfo head;
        currentbuffer.getHeader(&head);

        // set block = rblock of current block (from the header)
        block = head.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum)
{
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF)
    {
        // declare an instance of IndLeaf for rootBlockNum using appropriate
        // constructor
        IndLeaf leafblockbuffer(rootBlockNum);

        leafblockbuffer.releaseBlock();

        return SUCCESS;
    }
    else if (type == IND_INTERNAL)
    {
        // declare an instance of IndInternal for rootBlockNum using appropriate
        // constructor
        IndInternal internalBlockBuffer(rootBlockNum);

        // load the header of the block using BlockBuffer::getHeader().
        HeadInfo head;
        internalBlockBuffer.getHeader(&head);

        /*iterate through all the entries of the internalBlk and destroy the lChild
        of the first entry and rChild of all entries using BPlusTree::bPlusDestroy().
        (the rchild of an entry is the same as the lchild of the next entry.
         take care not to delete overlapping children more than once ) */

        for (int slot = 0; slot < head.numEntries; slot++)
        {
            InternalEntry entry;
            internalBlockBuffer.getEntry(&entry, slot);
            if (slot == 0)
            {
                bPlusDestroy(entry.lChild);
            }
            bPlusDestroy(entry.rChild);
        }

        internalBlockBuffer.releaseBlock();

        return SUCCESS;
    }
    else
    {
        // (block is not an index block.)
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId)
{

    AttrCatEntry attrCatBuf;
    int retVal = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    if (retVal != SUCCESS)
        return retVal;

    int rootblockNum = attrCatBuf.rootBlock;

    if (rootblockNum == -1)
    {
        return E_NOINDEX;
    }

    // find the leaf block to which insertion is to be done using the
    // findLeafToInsert() function

    int leafBlkNum = findLeafToInsert(rootblockNum, attrVal, attrCatBuf.attrType);

    // insert the attrVal and recId to the leaf block at blockNum using the
    // insertIntoLeaf() function.
    // declare a struct Index with attrVal = attrVal, block = recId.block and
    // slot = recId.slot to pass as argument to the function.
    Index leafEntry;
    leafEntry.attrVal = attrVal;
    leafEntry.block = recId.block;
    leafEntry.slot = recId.slot;

    retVal = insertIntoLeaf(relId, attrName, leafBlkNum, leafEntry);
    // NOTE: the insertIntoLeaf() function will propagate the insertion to the
    //       required internal nodes by calling the required helper functions
    //       like insertIntoInternal() or createNewRoot()

    if (retVal == E_DISKFULL)
    {
        // destroy the existing B+ tree by passing the rootBlock to bPlusDestroy().
        bPlusDestroy(attrCatBuf.rootBlock);

        attrCatBuf.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType)
{
    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF)
    {
        // declare an IndInternal object for block using appropriate constructor
        IndInternal internalBlk(blockNum);

        // get header of the block using BlockBuffer::getHeader()
        HeadInfo head;
        internalBlk.getHeader(&head);

        /* iterate through all the entries, to find the first entry whose
             attribute value >= value to be inserted.
             NOTE: the helper function compareAttrs() declared in BlockBuffer.h
                   can be used to compare two Attribute values. */
        int slot = 0;
        for (; slot < head.numEntries; slot++)
        {
            InternalEntry entry;
            internalBlk.getEntry(&entry, slot);
            int compVal = compareAttrs(entry.attrVal, attrVal, attrType);
            if (compVal >= 0)
                break;
        }

        if (slot == head.numEntries)
        {
            InternalEntry entry;
            internalBlk.getEntry(&entry, slot - 1);
            blockNum = entry.rChild;
        }
        else
        {
            InternalEntry entry;
            internalBlk.getEntry(&entry, slot);
            blockNum = entry.lChild;
        }
    }

    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    // declare an IndLeaf instance for the block using appropriate constructor
    IndLeaf leafblk(blockNum);

    HeadInfo blockHeader;
    // store the header of the leaf index block into blockHeader
    // using BlockBuffer::getHeader()
    leafblk.getHeader(&blockHeader);

    // the following variable will be used to store a list of index entries with
    // existing indices + the new index to insert
    Index indices[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array indices.
    Also insert `indexEntry` at appropriate position in the indices array maintaining
    the ascending order.
    - use IndLeaf::getEntry() to get the entry
    - use compareAttrs() declared in BlockBuffer.h to compare two Attribute structs
    */
    int slot = 0, i = 0;
    for (; slot < blockHeader.numEntries; i++, slot++)
    {
        Index entry;
        leafblk.getEntry(&entry, slot);
        int cmpval = compareAttrs(entry.attrVal, indexEntry.attrVal, attrCatBuf.attrType);
        if (cmpval > 0)
        {
            indices[i] = indexEntry;
            i++;
            break;
        }
        else indices[i] = entry;
    }

    if (i == slot)
        indices[i] = indexEntry;
    else
    {
        for(;slot < blockHeader.numEntries; i++, slot++){
            Index entry;
            leafblk.getEntry(&entry, slot);
            indices[i] = entry;
        }
    }    

    if (blockHeader.numEntries < MAX_KEYS_LEAF)
    {
        // (leaf block has not reached max limit)

        // increment blockHeader.numEntries and update the header of block
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        leafblk.setHeader(&blockHeader);

        // iterate through all the entries of the array `indices` and populate the
        // entries of block with them using IndLeaf::setEntry().
        for (slot = 0; slot < blockHeader.numEntries; slot++)
        {
            leafblk.setEntry(&indices[slot], slot);
        }

        return SUCCESS;
    }

    // If we reached here, the `indices` array has more than entries than can fit
    // in a single leaf index block. Therefore, we will need to split the entries
    // in `indices` between two leaf blocks. We do this using the splitLeaf() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitLeaf(blockNum, indices);

    if (newRightBlk == E_DISKFULL)
        return E_DISKFULL;

    int retVal;
    if (blockHeader.pblock != -1)
    {
        // insert the middle value from `indices` into the parent block using the

        // the middle value will be at index 31 (given by constant MIDDLE_INDEX_LEAF)

        // create a struct InternalEntry with attrVal = indices[MIDDLE_INDEX_LEAF].attrVal,
        // lChild = currentBlock, rChild = newRightBlk and pass it as argument to
        // the insertIntoInternalFunction as follows
        InternalEntry middleEntry;
        middleEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        middleEntry.rChild = newRightBlk;
        middleEntry.lChild = blockNum;

        retVal = insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
    }
    else
    {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        retVal = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
    if (retVal == E_DISKFULL)
        return retVal;
    else
        return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting
    IndLeaf rightBlk;
    int rightBlkNum = rightBlk.getBlockNum();

    if (rightBlkNum == E_DISKFULL)
        return E_DISKFULL;

    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block
    IndLeaf leftBlk(leafBlockNum);

    int leftBlkNum = leafBlockNum;

    HeadInfo leftBlkHeader, rightBlkHeader;

    // get the headers of left block and right block using BlockBuffer::getHeader()
    rightBlk.getHeader(&rightBlkHeader);
    leftBlk.getHeader(&leftBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;

    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlkHeader.rblock = rightBlkNum;

    leftBlk.setHeader(&leftBlkHeader);

    // set the first 32 entries of leftBlk = the first 32 entries of indices array
    // and set the first 32 entries of newRightBlk = the next 32 entries of
    // indices array using IndLeaf::setEntry().
    for (int slot = 0; slot < 32; slot++)
    {
        leftBlk.setEntry(&indices[slot], slot);
    }
    for (int slot = 32, i = 0; slot < 64; i++, slot++)
    {
        rightBlk.setEntry(&indices[slot], i);
    }

    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    // declare intBlk, an instance of IndInternal using constructor 2 for the block
    // corresponding to intBlockNum
    IndInternal intBlk(intBlockNum);

    HeadInfo blockHeader;
    // load blockHeader with header of intBlk using BlockBuffer::getHeader().
    intBlk.getHeader(&blockHeader);

    // declare internalEntries to store all existing entries + the new entry
    InternalEntry internalEntries[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array
    `internalEntries`. Insert `indexEntry` at appropriate position in the
    array maintaining the ascending order.
        - use IndInternal::getEntry() to get the entry
        - use compareAttrs() to compare two structs of type Attribute

    Update the lChild of the internalEntry immediately following the newly added
    entry to the rChild of the newly added entry.
    */
    int slot = 0, entryiterator = 0,insert = 0;
    for (; slot < blockHeader.numEntries; slot++, entryiterator++)
    {
        InternalEntry entry;
        intBlk.getEntry(&entry, slot);
        int cmpVal = compareAttrs(entry.attrVal, intEntry.attrVal, attrCatBuf.attrType);
        if (cmpVal > 0)
        {
            internalEntries[entryiterator] = intEntry;
            insert = entryiterator;
            entryiterator++;
            break;
        }
        else
            internalEntries[entryiterator] = entry;
    } 

    if (entryiterator == slot)
    {
        internalEntries[entryiterator] = intEntry;
        internalEntries[entryiterator - 1].rChild = intEntry.lChild;
    }
    else
    {
        
        for(;slot < blockHeader.numEntries;slot++, entryiterator++)
        {
            InternalEntry entry;
            intBlk.getEntry(&entry, slot);
            internalEntries[entryiterator] = entry;
        }
    }

    if(insert != 0)
        internalEntries[insert - 1].rChild = intEntry.lChild;
    if (insert < MAX_KEYS_INTERNAL);            
        internalEntries[insert + 1].lChild = intEntry.rChild; 

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL)
    {

        // increment blockheader.numEntries and update the header of intBlk
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        intBlk.setHeader(&blockHeader);

        // iterate through all entries in internalEntries array and populate the
        // entries of intBlk with them using IndInternal::setEntry().
        for (slot = 0; slot < blockHeader.numEntries; slot++)
            intBlk.setEntry(&internalEntries[slot], slot);
        return SUCCESS;
    }

    // If we reached here, the `internalEntries` array has more than entries than
    // can fit in a single internal index block. Therefore, we will need to split
    // the entries in `internalEntries` between two internal index blocks. We do
    // this using the splitInternal() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL)
    {

        // Using bPlusDestroy(), destroy the right subtree, rooted at intEntry.rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree
        bPlusDestroy(intEntry.rChild);

        return E_DISKFULL;
    }
    int retVal;

    if (blockHeader.pblock != -1)
    {
        // insert the middle value from `internalEntries` into the parent block
        // using the insertIntoInternal() function (recursively).

        // the middle value will be at index 50 (given by constant MIDDLE_INDEX_INTERNAL)

        // create a struct InternalEntry with lChild = current block, rChild = newRightBlk
        // and attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal
        // and pass it as argument to the insertIntoInternalFunction as follows
        InternalEntry middleEntry;
        middleEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        middleEntry.rChild = newRightBlk;
        middleEntry.lChild = intBlockNum;

        retVal = insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
    }
    else
    {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments
        retVal = createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
    }

    if (retVal == E_DISKFULL)
        return retVal;
    else
        return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[])
{
    // declare rightBlk, an instance of IndInternal using constructor 1 to obtain new
    // internal index block that will be used as the right block in the splitting
    IndInternal rightBlk;

    // declare leftBlk, an instance of IndInternal using constructor 2 to read from
    // the existing internal index block
    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = intBlockNum;

    if (rightBlkNum == E_DISKFULL)
    {
        //(failed to obtain a new internal index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()
    rightBlk.getHeader(&rightBlkHeader);
    leftBlk.getHeader(&leftBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;

    // and update the header of rightBlk using BlockBuffer::setHeader()
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlkHeader.rblock = rightBlkNum;

    // and update the header using BlockBuffer::setHeader()
    leftBlk.setHeader(&leftBlkHeader);

    /*
    - set the first 50 entries of leftBlk = index 0 to 49 of internalEntries
      array
    - set the first 50 entries of newRightBlk = entries from index 51 to 100
      of internalEntries array using IndInternal::setEntry().
      (index 50 will be moving to the parent internal index block)
    */
    for (int slot = 0; slot < 50; slot++)
    {
        leftBlk.setEntry(&internalEntries[slot], slot);
        rightBlk.setEntry(&internalEntries[slot + 50], slot);
    }

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild); /* block type of a child of any entry of the internalEntries array */

    for (int slot = 0; slot < MIDDLE_INDEX_INTERNAL; slot++)
    {
        // declare an instance of BlockBuffer to access the child block using
        // constructor 2
        if (type == IND_LEAF)
        {
            IndLeaf leafblk(internalEntries[slot + MIDDLE_INDEX_INTERNAL].lChild);

            HeadInfo head;
            leafblk.getHeader(&head);

            head.pblock = rightBlkNum;

            leafblk.setHeader(&head);
        }
        else
        {
            IndInternal internalBlk(internalEntries[slot + MIDDLE_INDEX_INTERNAL].lChild);

            HeadInfo head;
            internalBlk.getHeader(&head);

            head.pblock = rightBlkNum;

            internalBlk.setHeader(&head);
        }
        // update pblock of the block to rightBlkNum using BlockBuffer::getHeader()
        // and BlockBuffer::setHeader().
    }

    if (type == IND_LEAF)
    {
        IndLeaf leafblk(internalEntries[99].rChild);

        HeadInfo head;
        leafblk.getHeader(&head);

        head.pblock = rightBlkNum;

        leafblk.setHeader(&head);
    }
    else
    {
        IndInternal internalBlk(internalEntries[99].rChild);

        HeadInfo head;
        internalBlk.getHeader(&head);

        head.pblock = rightBlkNum;

        internalBlk.setHeader(&head);
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    // declare newRootBlk, an instance of IndInternal using appropriate constructor
    // to allocate a new internal index block on the disk
    IndInternal newRootBlk;

    int newRootBlkNum = newRootBlk.getBlockNum();

    if (newRootBlkNum == E_DISKFULL)
    {
        // (failed to obtain an empty internal index block because the disk is full)

        // Using bPlusDestroy(), destroy the right subtree, rooted at rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree
        bPlusDestroy(rChild);

        return E_DISKFULL;
    }

    // update the header of the new block with numEntries = 1 using
    // BlockBuffer::getHeader() and BlockBuffer::setHeader()
    HeadInfo blockhead;
    newRootBlk.getHeader(&blockhead);
    blockhead.numEntries = 1;
    newRootBlk.setHeader(&blockhead);

    // create a struct InternalEntry with lChild, attrVal and rChild from the
    // arguments and set it as the first entry in newRootBlk using IndInternal::setEntry()
    InternalEntry entry;
    entry.attrVal = attrVal;
    entry.lChild = lChild;
    entry.rChild = rChild;
    newRootBlk.setEntry(&entry, 0);

    // declare BlockBuffer instances for the `lChild` and `rChild` blocks using
    // appropriate constructor and update the pblock of those blocks to `newRootBlkNum`
    // using BlockBuffer::getHeader() and BlockBuffer::setHeader()
    if(StaticBuffer::getStaticBlockType(lChild) == IND_LEAF){
        IndLeaf leafBlk(lChild);
        leafBlk.getHeader(&blockhead);
        blockhead.pblock = newRootBlkNum;
        leafBlk.setHeader(&blockhead);

    }else{
       IndInternal internalBlk(lChild);
        internalBlk.getHeader(&blockhead);
        blockhead.pblock = newRootBlkNum;
        internalBlk.setHeader(&blockhead);
    }

    if(StaticBuffer::getStaticBlockType(rChild) == IND_INTERNAL){
        IndLeaf leafBlk(rChild);
        leafBlk.getHeader(&blockhead);
        blockhead.pblock = newRootBlkNum;
        leafBlk.setHeader(&blockhead);

    }else{
       IndInternal internalBlk(rChild);
        internalBlk.getHeader(&blockhead);
        blockhead.pblock = newRootBlkNum;
        internalBlk.setHeader(&blockhead);
    }

    // update rootBlock = newRootBlkNum for set the entry corresponding to `attrName`
    // in the attribute cache using AttrCacheTable::setAttrCatEntry().
    attrCatBuf.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId,attrName,&attrCatBuf);

    return SUCCESS;
}