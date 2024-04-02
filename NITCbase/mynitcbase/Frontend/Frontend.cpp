#include "Frontend.h"

#include <cstring>
#include <iostream>

int Frontend::create_table(char relname[ATTR_SIZE], int no_attrs, char attributes[][ATTR_SIZE], int type_attrs[])
{
  return Schema::createRel(relname, no_attrs, attributes, type_attrs);
}

int Frontend::drop_table(char relname[ATTR_SIZE])
{
  return Schema::deleteRel(relname);
}

int Frontend::open_table(char relname[ATTR_SIZE])
{
  return Schema::openRel(relname);
}

int Frontend::close_table(char relname[ATTR_SIZE])
{
  return Schema::closeRel(relname);
}

int Frontend::alter_table_rename(char relname_from[ATTR_SIZE], char relname_to[ATTR_SIZE])
{
  return Schema::renameRel(relname_from, relname_to);
}

int Frontend::alter_table_rename_column(char relname[ATTR_SIZE], char attrname_from[ATTR_SIZE],
                                        char attrname_to[ATTR_SIZE])
{
  return Schema::renameAttr(relname, attrname_from, attrname_to);
}

int Frontend::create_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE])
{
  return Schema::createIndex(relname, attrname);
  ;
}

int Frontend::drop_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE])
{

  return Schema::dropIndex(relname, attrname);
}

int Frontend::insert_into_table_values(char relname[ATTR_SIZE], int attr_count, char attr_values[][ATTR_SIZE])
{
  return Algebra::insert(relname, attr_count, attr_values);
}

int Frontend::select_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE])
{
  int ret = Algebra::project(relname_source, relname_target);
  return ret;
}

int Frontend::select_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                      char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE])
{
  return Algebra::select(relname_source, relname_target, attribute, op, value);
}

int Frontend::select_attrlist_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                         int attr_count, char attr_list[][ATTR_SIZE])
{

  return Algebra::project(relname_source, relname_target, attr_count, attr_list);
}

int Frontend::select_attrlist_from_table_where(
    char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
    int attr_count, char attr_list[][ATTR_SIZE],
    char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE])
{

  int retval = Algebra::select(relname_source, "temp", attribute, op, value);

  if (retval != SUCCESS)
    return retval;

  int relid = OpenRelTable::openRel("temp");

  if (relid < 0 || relid >= MAX_OPEN)
  {
    Schema::deleteRel("temp");
    return relid;
  }

  retval = Algebra::project("temp", relname_target, attr_count, attr_list);

  Schema::closeRel("temp");
  Schema::deleteRel("temp");

  return retval;
}

int Frontend::select_from_join_where(
    char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
    char relname_target[ATTR_SIZE],
    char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE])
{

  // Call join() method of the Algebra Layer with correct arguments

  return Algebra::join(relname_source_one, relname_source_two,
                       relname_target, join_attr_one, join_attr_two);
}

int Frontend::select_attrlist_from_join_where(
    char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
    char relname_target[ATTR_SIZE], char join_attr_one[ATTR_SIZE],
    char join_attr_two[ATTR_SIZE], int attr_count, char attr_list[][ATTR_SIZE])
{

  // Call join() method of the Algebra Layer with correct arguments to
  // create a temporary target relation with name TEMP.

  // TEMP results from the join of the two source relation (and hence it
  // contains all attributes of the source relations except the join attribute
  // of the second source relation)

  int retVal = Algebra::join(relname_source_one, relname_source_two,
                             "temp_tar_rel", join_attr_one, join_attr_two);

  // Return Error values, if not successful
  if (retVal != SUCCESS)
    return retVal;

  // Open the TEMP relation using OpenRelTable::openRel()
  int targetrelId = OpenRelTable::openRel("temp_tar_rel");
  // if open fails, delete TEMP relation using Schema::deleteRel() and
  // return the error code
  if (targetrelId < 0 || targetrelId >= MAX_OPEN)
  {
    Schema::deleteRel("temp_tar_rel");
    return targetrelId;
  }

  // Call project() method of the Algebra Layer with correct arguments to
  // create the actual target relation from the TEMP relation.
  // (The final target relation contains only those attributes mentioned in attr_list)

  retVal = Algebra::project("temp_tar_rel", relname_target, attr_count, attr_list);

  // close the TEMP relation using OpenRelTable::closeRel()
  // delete the TEMP relation using Schema::deleteRel()

  Schema::closeRel("temp_tar_rel");
  Schema::deleteRel("temp_tar_rel");

  return retVal;
}

int Frontend::custom_function(int argc, char argv[][ATTR_SIZE])
{
  // argc gives the size of the argv array
  // argv stores every token delimited by space and comma
  // implement whatever you desire
  int t = argc != 1 && strcpy(argv[0], "ls") != 0;

  if (t == 1) 
  {
    printf("Syntax Error\n");
    return 0;
  }    

  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  RecBuffer relCatBuffer(RELCAT_BLOCK);

  HeadInfo head;

  relCatBuffer.getHeader(&head);

  unsigned char slotMap[head.numSlots];

  relCatBuffer.getSlotMap(slotMap);


  for (int slot = 0; slot < head.numSlots; slot++)
  {
    if (slotMap[slot] == SLOT_OCCUPIED)
    {
      Attribute record[RELCAT_NO_ATTRS];
      relCatBuffer.getRecord(record, slot);

      printf("%s\n", record[RELCAT_REL_NAME_INDEX].sVal);
    }
  }

  printf("\n");

  return SUCCESS;
}
