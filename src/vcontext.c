#include "redis.h"
#include "viki.h"

typedef struct vcontextData {
  int added;
  long allow_count, block_count, index_offset;
  robj **filter_objects, **index_objects;
  dict **allows, **blocks, **filters, **indexes;
} vcontextData;

void vcontextWithFilters(redisClient *c, long filter_count, long index_count, vcontextData *data);
void vcontextWithoutFilters(redisClient *c, long index_count, vcontextData *data);

// vcontext allow_count [allows] block_count [blocks] filter_count [filters] index_count [indexes]
void vcontextCommand(redisClient *c) {
  long allow_count, block_count, block_offset, filter_count, filter_offset, index_count, index_offset;
  vcontextData *data;
  void *replylen;

  if ((getLongFromObjectOrReply(c, c->argv[1], &allow_count, NULL) != REDIS_OK)) { return; }
  block_offset = 2 + allow_count;
  if ((getLongFromObjectOrReply(c, c->argv[block_offset], &block_count, NULL) != REDIS_OK)) { return; }
  filter_offset = 3 + allow_count + block_count;
  if ((getLongFromObjectOrReply(c, c->argv[filter_offset], &filter_count, NULL) != REDIS_OK)) { return; }
  index_offset = 4 + allow_count + block_count + filter_count;
  if ((getLongFromObjectOrReply(c, c->argv[index_offset], &index_count, NULL) != REDIS_OK)) { return; }

  data = zmalloc(sizeof(*data));
  data->filters = NULL;
  data->filter_objects = NULL;
  data->indexes = zmalloc(sizeof(dict*) * index_count);
  data->index_objects = zmalloc(sizeof(robj*) * index_count);
  data->added = 0;
  data->index_offset = index_offset;

  replylen = addDeferredMultiBulkLength(c);

  data->allows = loadSetArray(c, 2, &allow_count);
  data->allow_count = allow_count;

  data->blocks = loadSetArray(c, block_offset+1, &block_count);
  data->block_count = block_count;

  for(int i = 0; i < index_count; ++i) {
    if ((data->index_objects[i] = lookupKey(c->db, c->argv[i+index_offset+1])) != NULL && checkType(c, data->index_objects[i], REDIS_SET)) { goto reply; }
    data->indexes[i] = data->index_objects[i] == NULL ? NULL : (dict*)data->index_objects[i]->ptr;
  }

  if (filter_count > 0) {
    data->filters = zmalloc(sizeof(dict*) * filter_count);
    data->filter_objects = zmalloc(sizeof(robj*) * filter_count);
    for(int i = 0; i < filter_count; ++i) {
      if ((data->filter_objects[i] = lookupKey(c->db, c->argv[i+filter_offset+1])) == NULL || checkType(c, data->filter_objects[i], REDIS_SET)) { goto reply; }
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
  if (data->allows != NULL) { zfree(data->allows); }
  if (data->blocks != NULL) { zfree(data->blocks); }
  if (data->filters != NULL) { zfree(data->filters); }
  if (data->filter_objects != NULL) { zfree(data->filter_objects); }
  zfree(data->indexes);
  zfree(data->index_objects);
  zfree(data);
}

void vcontextWithFilters(redisClient *c, long filter_count, long index_count, vcontextData *data) {
  robj *item;
  dict *index;
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  long index_offset = data->index_offset;
  dict **allows = data->allows;
  dict **blocks = data->blocks;
  dict **indexes = data->indexes;
  dict **filters = data->filters;
  robj **filter_objects = data->filter_objects;

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
    if (!heldback2(allow_count, allows, block_count, blocks, item)) {
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
        addReplyBulk(c, c->argv[i+index_offset+1]);
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
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  long index_offset = data->index_offset;
  dict **allows = data->allows;
  dict **blocks = data->blocks;
  dict **indexes = data->indexes;
  robj **index_objects = data->index_objects;
  setTypeIterator *si = zmalloc(sizeof(setTypeIterator));

  for(int i = 0; i < index_count; ++i) {
    if (indexes[i] == NULL) { continue; }
    si->subject = index_objects[i];
    si->encoding = si->subject->encoding;
    si->di = dictGetIterator(indexes[i]);
    while((setTypeNext(si, &item, NULL)) != -1) {
      if (!heldback2(allow_count, allows, block_count, blocks, item)) {
        ++(data->added);
        addReplyBulk(c, c->argv[i+index_offset+1]);
        break;
      }
    }
    dictReleaseIterator(si->di);
  }
  zfree(si);
}
