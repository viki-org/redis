#include "redis.h"
#include "viki.h"

typedef struct vcontextData {
  int added;
  robj **filter_objects, **index_objects;
  dict *cap, **filters, **indexes;
} vcontextData;

void vcontextWithFilters(redisClient *c, long filter_count, long index_count, vcontextData *data);
void vcontextWithoutFilters(redisClient *c, long index_count, vcontextData *data);

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
  vcontextData *data;
  robj *cap;
  void *replylen;

  if ((getLongFromObjectOrReply(c, c->argv[1], &filter_count, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[2 + filter_count], &index_count, NULL) != REDIS_OK)) { return; }
  if ((filter_count + index_count + 1)  > (c->argc-3) || index_count == 0) {
    addReply(c,shared.syntaxerr);
    return;
  }

  data = zmalloc(sizeof(*data));
  data->filters = NULL;
  data->filter_objects = NULL;
  data->indexes = zmalloc(sizeof(dict*) * index_count);
  data->index_objects = zmalloc(sizeof(robj*) * index_count);
  data->added = 0;

  replylen = addDeferredMultiBulkLength(c);

  for(int i = 0; i < index_count; ++i) {
    if ((data->index_objects[i] = lookupKey(c->db, c->argv[i+3+filter_count])) != NULL && checkType(c, data->index_objects[i], REDIS_SET)) { goto reply; }
    data->indexes[i] = data->index_objects[i] == NULL ? NULL : (dict*)data->index_objects[i]->ptr;
  }

  if ((cap = lookupKey(c->db, c->argv[filter_count + index_count + 3])) != NULL && checkType(c, cap, REDIS_SET)) { goto reply; }
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;

  if (filter_count > 0) {
    data->filters = zmalloc(sizeof(dict*) * filter_count);
    data->filter_objects = zmalloc(sizeof(robj*) * filter_count);
    for(int i = 0; i < filter_count; ++i) {
      if ((data->filter_objects[i] = lookupKey(c->db, c->argv[i+2])) == NULL || checkType(c, data->filter_objects[i], REDIS_SET)) { goto reply; }
    }
    qsort(data->filter_objects, filter_count, sizeof(robj*), qsortCompareSetsByCardinality);
    for (int i = 0; i < filter_count; ++i) {
      data->filters[i] = (dict*)data->filter_objects[i]->ptr;
    }
    vcontextWithFilters(c, filter_count, index_count, data);
  } else {
    vcontextWithoutFilters(c, index_count, data);
  }

reply:
  setDeferredMultiBulkLength(c, replylen, data->added);
  if (data->filters != NULL) { zfree(data->filters); }
  if (data->filter_objects != NULL) { zfree(data->filter_objects); }
  zfree(data->indexes);
  zfree(data->index_objects);
  zfree(data);
}

void vcontextWithFilters(redisClient *c, long filter_count, long index_count, vcontextData *data) {
  robj *item;

  dict *index;
  dict **indexes = data->indexes;
  dict **filters = data->filters;
  robj **filter_objects = data->filter_objects;
  dict *cap = data->cap;

  robj *dstobj = createSetObject();
  dict *dstset = (dict*)dstobj->ptr;

  setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
  si->subject = filter_objects[0];
  si->encoding = si->subject->encoding;
  si->di = dictGetIterator(filters[0]);

  while((setTypeNext(si, &item, NULL)) != -1) {
    for(int j = 1; j < filter_count; ++j) {
      if (!isMember(filters[j], item)) { goto next; }
    }
    if (!heldback(cap, NULL, item)) {
      dictAdd(dstset, item, NULL);
      incrRefCount(item);
    }
  next:
    item = NULL;
  }
  dictReleaseIterator(si->di);

  for(int i = 0; i < index_count; ++i) {
    index = indexes[i];
    if (index == NULL) { continue; }
    si->subject = data->index_objects[i];
    si->encoding = si->subject->encoding;
    si->di = dictGetIterator(index);

    while((setTypeNext(si, &item, NULL)) != -1) {
      if (isMember(dstset, item)) {
        ++(data->added);
        addReplyBulk(c, c->argv[i+3+filter_count]);
        break;
      }
    }
    dictReleaseIterator(si->di);
  }
  zfree(si);
  decrRefCount(dstobj);
}

void vcontextWithoutFilters(redisClient *c, long index_count, vcontextData *data) {
  robj *item;

  dict **indexes = data->indexes;
  robj **index_objects = data->index_objects;
  dict *cap = data->cap;
  setTypeIterator *si = zmalloc(sizeof(setTypeIterator));

  for(int i = 0; i < index_count; ++i) {
    if (indexes[i] == NULL) { continue; }
    si->subject = index_objects[i];
    si->encoding = si->subject->encoding;
    si->di = dictGetIterator(indexes[i]);
    while((setTypeNext(si, &item, NULL)) != -1) {
      if (!heldback(cap, NULL, item)) {
        ++(data->added);
        addReplyBulk(c, c->argv[i+3]);
        break;
      }
    }
    dictReleaseIterator(si->di);
  }
  zfree(si);
}