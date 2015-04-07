#include "redis.h"
#include "viki.h"

typedef struct vcontextData {
  int added;
  long allow_count, block_count, index_offset;
  robj **index_objects;
  dict **allows, **blocks, **indexes;
} vcontextData;

typedef struct indexNode {
  int i;
  dict *index;
  struct indexNode *next;
} indexNode;

void vcontextWithFilters(redisClient *c, long filter_offset, long filter_count, long index_count, vcontextData *data);
void vcontextWithOneFilter(redisClient *c, long filter_offset, long index_count, vcontextData *data);
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

  if (filter_count == 0) {
    vcontextWithoutFilters(c, index_count, data);
  } else if (filter_count == 1) {
    vcontextWithOneFilter(c, filter_offset, index_count, data);
  } else {
    vcontextWithFilters(c, filter_offset, filter_count, index_count, data);
  }

reply:
  setDeferredMultiBulkLength(c, replylen, data->added);
  if (data->allows != NULL) { zfree(data->allows); }
  if (data->blocks != NULL) { zfree(data->blocks); }
  zfree(data->indexes);
  zfree(data->index_objects);
  zfree(data);
}

void vcontextWithOneFilter(redisClient *c, long filter_offset, long index_count, vcontextData *data) {
  robj *item;
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  long index_offset = data->index_offset;
  dict **allows = data->allows;
  dict **blocks = data->blocks;
  dict **indexes = data->indexes;

  robj *filter_object;
  if ((filter_object = lookupKey(c->db, c->argv[filter_offset+1])) == NULL || checkType(c, filter_object, REDIS_SET)) { return; }

  setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
  si->subject = filter_object;
  si->encoding = si->subject->encoding;
  si->di = dictGetIterator((dict*)filter_object->ptr);

  indexNode *head = zmalloc(sizeof(indexNode));
  head->i = -1;
  indexNode *last = head;
  for(int i = 0; i < index_count; ++i) {
    if (indexes[i] == NULL) { continue; }
    indexNode *node = zmalloc(sizeof(indexNode));
    node->i = i;
    node->index = indexes[i];
    last->next = node;
    last = node;
  }
  last->next = NULL;

  while((setTypeNext(si, &item, NULL)) != -1) {
    if (heldback(allow_count, allows, block_count, blocks, item)) { continue; }
    indexNode *last = head;
    for (indexNode *n = head->next; n != NULL; n = n->next) {
      if (isMember(n->index, item)) {
        ++(data->added);
        addReplyBulk(c, c->argv[n->i+index_offset+1]);
        last->next = n->next;
        zfree(n);
        if (head->next == NULL) { goto cleanup; }
      } else {
        last = n;
      }
    }
  }

cleanup:

  {
    indexNode *n = head;
    while (1) {
       if (n == NULL) { break; }
       indexNode *curr = n;
       n = curr->next;
       zfree(curr);
    }
  }
  dictReleaseIterator(si->di);
  zfree(si);
}

void vcontextWithFilters(redisClient *c, long filter_offset, long filter_count, long index_count, vcontextData *data) {
  robj *item;
  dict *index;
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  long index_offset = data->index_offset;
  dict **allows = data->allows;
  dict **blocks = data->blocks;
  dict **indexes = data->indexes;

  dict **filters = zmalloc(sizeof(dict*) * filter_count);
  robj **filter_objects = zmalloc(sizeof(robj*) * filter_count);

  for(int i = 0; i < filter_count; ++i) {
    if ((filter_objects[i] = lookupKey(c->db, c->argv[i+filter_offset+1])) == NULL || checkType(c, filter_objects[i], REDIS_SET)) { goto cleanup; }
  }
  qsort(filter_objects, filter_count, sizeof(robj*), qsortCompareSetsByCardinality);
  for (int i = 0; i < filter_count; ++i) {
    filters[i] = (dict*)filter_objects[i]->ptr;
  }

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
    if (!heldback(allow_count, allows, block_count, blocks, item)) {
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

cleanup:
  zfree(filters);
  zfree(filter_objects);
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
      if (!heldback(allow_count, allows, block_count, blocks, item)) {
        ++(data->added);
        addReplyBulk(c, c->argv[i+index_offset+1]);
        break;
      }
    }
    dictReleaseIterator(si->di);
  }
  zfree(si);
}
