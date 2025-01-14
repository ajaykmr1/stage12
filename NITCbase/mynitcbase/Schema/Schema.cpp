#include "Schema.h"

#include <cmath>
#include <cstring>

int Schema::openRel(char relName[ATTR_SIZE])
{
  int ret = OpenRelTable::openRel(relName);

  // the OpenRelTable::openRel() function returns the rel-id if successful
  // a valid rel-id will be within the range 0 <= relId < MAX_OPEN and any
  // error codes will be negative
  if (ret < 12 && ret >= 0)
  {
    return SUCCESS;
  }

  // otherwise it returns an error message
  return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
  int t = (strcmp(relName, RELCAT_RELNAME) && strcmp(relName, ATTRCAT_RELNAME));
  if (t == 0 /* relation is relation catalog or attribute catalog */)
  {
    return E_NOTPERMITTED;
  }

  // this function returns the rel-id of a relation if it is open or
  // E_RELNOTOPEN if it is not. we will implement this later.
  int relId = OpenRelTable::getRelId(relName);

  if (relId < 0)
  {
    return E_RELNOTOPEN;
  }

  return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE])
{
  int t = (strcmp(oldRelName, RELCAT_RELNAME) && strcmp(oldRelName, ATTRCAT_RELNAME)) &&
          (strcmp(newRelName, RELCAT_RELNAME) && strcmp(newRelName, ATTRCAT_RELNAME));
  if (t == 0)
  {
    return E_NOTPERMITTED;
  }

  int relId = OpenRelTable::getRelId(oldRelName);

  if (relId != E_RELNOTOPEN)
  {
    return E_RELOPEN;
  }

  int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
  return retVal;
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName)
{

  int t = (strcmp(relName, RELCAT_RELNAME) && strcmp(relName, ATTRCAT_RELNAME));
  if (t == 0)
  {
    return E_NOTPERMITTED;
  }

  int relId = OpenRelTable::getRelId(relName);

  if (relId != E_RELNOTOPEN)
  {
    return E_RELOPEN;
  }

  int retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
  return retVal;
}


int Schema::createRel(char relName[], int nAttrs, char attrs[][ATTR_SIZE], int attrtype[])
{

  // declare variable relNameAsAttribute of type Attribute
  Attribute relNameAsAttribute;

  // copy the relName into relNameAsAttribute.sVal
  strcpy(relNameAsAttribute.sVal, relName);

  // declare a variable targetRelId of type RecId
  RecId targetRelId;

  // Reset the searchIndex using RelCacheTable::resetSearhIndex()
  // Search the relation catalog (relId given by the constant RELCAT_RELID)
  int ret = RelCacheTable::resetSearchIndex(RELCAT_RELID);
  if (ret != SUCCESS)
    return ret;

  targetRelId = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAsAttribute, EQ);

  if (targetRelId.block != -1 && targetRelId.slot != -1)
    return E_RELEXIST;

  // compare every pair of attributes of attrNames[] array
  // if any attribute names have same string value,
  //     return E_DUPLICATEATTR (i.e 2 attributes have same value)
  for (int attrindex = 0; attrindex < nAttrs-1; attrindex++)
  {
    if (strcmp(attrs[attrindex],attrs[attrindex+1]) == 0)
      return E_DUPLICATEATTR;
  }

  /* declare relCatRecord of type Attribute which will be used to store the
     record corresponding to the new relation which will be inserted
     into relation catalog */
  Attribute relCatRecord[RELCAT_NO_ATTRS];

  strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,relName);
  relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
  relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
  relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = (int)(2016/(16 * nAttrs + 1));
  // (number of slots is calculated as specified in the physical layer docs)

  int  retVal = BlockAccess::insert(RELCAT_RELID,relCatRecord);

  if(retVal != SUCCESS)
    return retVal;


  for(int attrindex=0;attrindex<nAttrs;attrindex++)
  {
    Attribute attrCatRecord[6];

    strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
    strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[attrindex]);
    attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[attrindex]; 
    attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = attrindex;

    retVal = BlockAccess::insert(ATTRCAT_RELID,attrCatRecord);

    if(retVal != SUCCESS)
    {
      Schema::deleteRel(relName);
      return E_DISKFULL;
    }  
  }

  return SUCCESS;
}

int Schema::deleteRel(char *relName) {

  int t = (strcmp(relName, RELCAT_RELNAME) && strcmp(relName, ATTRCAT_RELNAME));
  if (t == 0 )
  {
    return E_NOTPERMITTED;
  }

    // get the rel-id using appropriate method of OpenRelTable class by
    // passing relation name as argument
  int  relid = OpenRelTable::getRelId(relName);
  if (relid != E_RELNOTOPEN)
  {
    return E_RELOPEN;
  }


  int retval = BlockAccess::deleteRelation(relName);

  return retval;

    /* the only that should be returned from deleteRelation() is E_RELNOTEXIST.
       The deleteRelation call may return E_OUTOFBOUND from the call to
       loadBlockAndGetBufferPtr, but if your implementation so far has been
       correct, it should not reach that point. That error could only occur
       if the BlockBuffer was initialized with an invalid block number.
    */
}

int Schema::createIndex(char relName[ATTR_SIZE],char attrName[ATTR_SIZE]){
  int t = (strcmp(relName, RELCAT_RELNAME) && strcmp(relName, ATTRCAT_RELNAME));
  if (t == 0)
  {
    return E_NOTPERMITTED;
  }
    
  int relId = OpenRelTable::getRelId(relName);  
    // get the relation's rel-id using OpenRelTable::getRelId() method

  if (relId == E_RELNOTOPEN) 
    return E_RELNOTOPEN;

  return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char *relName, char *attrName) {
  int t = (strcmp(relName, RELCAT_RELNAME) && strcmp(relName, ATTRCAT_RELNAME));
  if (t == 0)
  {
    return E_NOTPERMITTED;
  }
    
  int relId = OpenRelTable::getRelId(relName);  

  if (relId == E_RELNOTOPEN) 
    return E_RELNOTOPEN;

  AttrCatEntry attrCatBuf;  
  
  int retVal = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

  if(retVal != SUCCESS) return E_ATTRNOTEXIST;

    int rootBlock = attrCatBuf.rootBlock;

    if (rootBlock == -1) {
        return E_NOINDEX;
    }

    // destroy the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
    BPlusTree::bPlusDestroy(rootBlock);

    // set rootBlock = -1 in the attribute cache entry of the attribute using
    attrCatBuf.rootBlock = -1;

    // AttrCacheTable::setAttrCatEntry()
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    return SUCCESS;
}