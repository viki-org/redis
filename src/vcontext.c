#include "redis.h"
#include "viki.h"

void vcontextWithFilters(redisClient *c, long filter_count, long index_count);

int *vcontextGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
  int i, filter_count, index_count, total_count, *keys;
  REDIS_NOTUSED(cmd);
  REDIS_NOTUSED(flags);

  filter_count = atoi(argv[1]->ptr);
  index_count = atoi(argv[2 + filter_count]->ptr);
  total_count = filter_count + index_count + 1; //cap

  // Sanity check. Don't return any key if the command is going to reply with syntax error.
  if (total_count > (argc-4) || index_count == 0) {
    *numkeys = 0;
    return NULL;
  }
  keys = zmalloc(sizeof(int)*total_count);
  for (i = 0; i < filter_count; ++i) {
    keys[i] = i + 1;
  }
  for (i = filter_count + 1; i < index_count; ++i) {
    keys[i] = i + 1;
  }
  *numkeys = total_count;
  return keys;
}

void vcontextCommand(redisClient *c) {
  long filter_count, index_count;

  if ((getLongFromObjectOrReply(c, c->argv[1], &filter_count, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[2 + filter_count], &index_count, NULL) != REDIS_OK)) { return; }

  if ((filter_count + index_count + 1)  > (c->argc-3) || index_count == 0) {
    addReply(c,shared.syntaxerr);
    return;
  }

  if (filter_count > 0) {
    vcontextWithFilters(c, filter_count, index_count);
  }
}

void vcontextWithFilters(redisClient *c, long filter_count, long index_count) {
  int added = 0;
  void *replylen;
  robj **filter_objects, **index_objects, *item, *cap_object = NULL;
  dict *cap, **filters, **indexes, *index = NULL;
  int64_t intobj;

  replylen = addDeferredMultiBulkLength(c);
  filter_objects = zmalloc(sizeof(robj*) * filter_count);
  filters = zmalloc(sizeof(dict*) * filter_count);
  index_objects = zmalloc(sizeof(robj*) * index_count);
  indexes = zmalloc(sizeof(dict*) * index_count);

  for(int i = 0; i < filter_count; ++i) {
    if ((filter_objects[i] = lookupKey(c->db, c->argv[i+2])) == NULL || checkType(c, filter_objects[i], REDIS_SET)) { goto reply; }
  }
  for(int i = 0; i < index_count; ++i) {
    if ((index_objects[i] = lookupKey(c->db, c->argv[i+3+filter_count])) == NULL || checkType(c, index_objects[i], REDIS_SET)) { goto reply; }
  }
  if ((cap_object = lookupKey(c->db, c->argv[filter_count + index_count + 3])) != NULL && checkType(c, cap_object, REDIS_SET)) { goto reply; }

  cap = (cap_object == NULL) ? NULL : (dict*)cap_object->ptr;

  qsort(filter_objects, filter_count, sizeof(robj*), qsortCompareSetsByCardinality);
  for (int i = 0; i < filter_count; ++i) {
    filters[i] = (dict*)filter_objects[i]->ptr;
  }
  for (int i = 0; i < index_count; ++i) {
    indexes[i] = (dict*)index_objects[i]->ptr;
  }

  setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
  si->subject = filter_objects[0];
  si->encoding = si->subject->encoding;
  si->di = dictGetIterator(filters[0]);

  for(int i = 0; i < index_count; ++i) {
    index = indexes[i];
    while((setTypeNext(si, &item, &intobj)) != -1) {
      for(int j = 1; j < filter_count; ++j) {
        if (!isMember(filters[j], item)) { goto next; }
      }
      if (!heldback(cap, NULL, item) && isMember(index, item)) {
        ++added;
        addReplyBulk(c, c->argv[i+3+filter_count]);
        break;
      }
  next:
      item = NULL;
    }
  }

  dictReleaseIterator(si->di);
  zfree(si);

reply:
  setDeferredMultiBulkLength(c, replylen, added);
  if (filters != NULL) { zfree(filters); }
  if (filter_objects != NULL) { zfree(filter_objects); }
  if (indexes != NULL) { zfree(indexes); }
  if (index_objects != NULL) { zfree(index_objects); }
}