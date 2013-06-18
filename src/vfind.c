#include "redis.h"
#include "viki.h"
#include <math.h>

typedef struct vfindData {
  int desc, found, added, include_blocked;
  long allow_count, block_count, filter_count, offset, count, up_to;
  robj *detail_field;
  robj **filter_objects;
  dict **allows, **blocks, **filters;
  zskiplistNode *ln;
  zset *zset;
} vfindData;

zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);

void vfindByZWithFilters(redisClient *c, vfindData *data);
void vfindByFilters(redisClient *c, vfindData *data);

static void initializeZsetIterator(vfindData *data);

int *vfindGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys, int flags) {
  REDIS_NOTUSED(cmd);
  REDIS_NOTUSED(flags);

  int i, num, offset, *keys;
  int allow_count = atoi(argv[8]->ptr);
  int block_offset = 9 + allow_count;
  int block_count = atoi(argv[block_offset]->ptr);
  int filter_offset = 10 + allow_count + block_count;
  int filter_count = atoi(argv[filter_offset]->ptr);

  num = 1 + block_count + allow_count + filter_count;  //1 is for the zset
  keys = zmalloc(sizeof(int)*num);
  // zset key position
  keys[0] = 1;
  offset = 1;

  for (i = 0; i < allow_count; ++i) {
    keys[offset++] = 9+i;
  }
  for (i = 0; i < block_count; ++i) {
    keys[offset++] = block_offset+i;
  }
  for (i = 0; i < filter_count; ++i) {
    keys[offset++] = filter_offset+i;
  }
  *numkeys = num;
  return keys;
}

// Prepares all the sets and calls vfindByFilters or vfindByZWithFilters
// depending on the ratio of the input set and the smallest filter set
// Syntax:
// vfind zset offset count upto direction include_blocked detail_field allow_count [allows] block_count [blocks] filter_count [filter keys]
void vfindCommand(redisClient *c) {
  long allow_count, block_count, block_offset, filter_count, filter_offset;
  void *replylen;
  long offset, count, up_to;
  robj *items, *direction, *include_blocked, *data_field;
  vfindData *data;

  // All the data checks
  if ((items = lookupKey(c->db, c->argv[1])) == NULL) {
    addReplyMultiBulkLen(c, 1);
    addReplyLongLong(c, 0);
    return;
  }
  if (checkType(c, items, REDIS_ZSET)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[2], &offset, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[3], &count, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[4], &up_to, NULL) != REDIS_OK)) { return; }
  direction = c->argv[5];
  include_blocked = c->argv[6];
  data_field = c->argv[7];

  if ((getLongFromObjectOrReply(c, c->argv[8], &allow_count, NULL) != REDIS_OK)) { return; }
  block_offset = 9 + allow_count;
  if ((getLongFromObjectOrReply(c, c->argv[block_offset], &block_count, NULL) != REDIS_OK)) { return; }
  filter_offset = 10 + allow_count + block_count;
  if ((getLongFromObjectOrReply(c, c->argv[filter_offset], &filter_count, NULL) != REDIS_OK)) { return; }

  data = zmalloc(sizeof(*data));
  data->detail_field = data_field;
  data->filters = NULL;
  data->filter_objects = NULL;
  data->offset = offset;
  data->count = count;
  data->up_to = up_to;
  data->added = 0;
  data->found = 0;
  data->desc = 1;
  data->include_blocked = 0;

  replylen = addDeferredMultiBulkLength(c);

  zsetConvert(items, REDIS_ENCODING_SKIPLIST);
  data->zset = items->ptr;

  if (!strcasecmp(direction->ptr, "asc")) { data->desc = 0; }
  // The keyword for blocked items is "withblocked"
  if (!strcasecmp(include_blocked->ptr, "withblocked")) { data->include_blocked = 1; }

  data->allows = loadSetArray(c, 9, &allow_count);
  data->allow_count = allow_count;

  data->blocks = loadSetArray(c, block_offset+1, &block_count);
  data->block_count = block_count;

  data->filter_count = filter_count;
  if (filter_count != 0) {
    data->filter_objects = zmalloc(sizeof(robj*) * filter_count);
    data->filters = zmalloc(sizeof(dict*) * filter_count);
    for(int i = 0; i < filter_count; ++i) {
      if ((data->filter_objects[i] = lookupKey(c->db, c->argv[i+filter_offset+1])) == NULL || checkType(c, data->filter_objects[i], REDIS_SET)) { goto reply; }
    }
    qsort(data->filter_objects, filter_count, sizeof(robj*), qsortCompareSetsByCardinality);
    for (int i = 0; i < filter_count; ++i) {
      data->filters[i] = (dict*)data->filter_objects[i]->ptr;
    }

    // Size of smallest filter
    int size = dictSize(data->filters[0]);
    int ratio = zsetLength(items) / size;

    if ((size < 100 && ratio > 1) || (size < 500 && ratio > 2) || (size < 2000 && ratio > 3)) {
      vfindByFilters(c, data);
      goto reply;
    }
  }
  initializeZsetIterator(data);
  vfindByZWithFilters(c, data);

  reply:
  addReplyLongLong(c, data->found);
  setDeferredMultiBulkLength(c, replylen, (data->added) + 1);
  if (data->allows != NULL) { zfree(data->allows); }
  if (data->blocks != NULL) { zfree(data->blocks); }
  if (data->filters != NULL) { zfree(data->filters); }
  if (data->filter_objects != NULL) { zfree(data->filter_objects); }
  zfree(data);
}

// Used if the smallest filter has a small size compared to zset's size
void vfindByFilters(redisClient *c, vfindData *data) {
  zset *dstzset;
  robj *item;
  zskiplist *zsl;
  zskiplistNode *ln;

  int desc = data->desc;
  long offset = data->offset;
  long count = data->count;
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  long filter_count = data->filter_count;
  int found = 0;
  int added = 0;
  zset *zset = data->zset;
  dict **allows = data->allows;
  dict **blocks = data->blocks;
  dict **filters = data->filters;
  robj *detail_field = data->detail_field;
  int64_t intobj;
  double score;

  robj *dstobj = createZsetObject();
  dstzset = dstobj->ptr;
  zsl = dstzset->zsl;

  setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
  si->subject = data->filter_objects[0];
  si->encoding = si->subject->encoding;
  si->di = dictGetIterator(filters[0]);

  while((setTypeNext(si, &item, &intobj)) != -1) {
    for(int i = 1; i < filter_count; ++i) {
      if (!isMember(filters[i], item)) { goto next; }
    }

    item->blocked = heldback2(allow_count, allows, block_count, blocks, item);
    if (item->blocked && !data->include_blocked) { goto next; }

    dictEntry *de;
    if ((de = dictFind(zset->dict, item)) != NULL) {
      score = *(double*)dictGetVal(de);
      zslInsert(zsl, score, item);
      incrRefCount(item);
      dictAdd(dstzset->dict, item, &score);
      incrRefCount(item);
      ++found;
    }
    next:
    item = NULL;
  }

  dictReleaseIterator(si->di);
  zfree(si);

  if (found > offset) {
    if (desc) {
      ln = offset == 0 ? zsl->tail : zslGetElementByRank(zsl, found - offset);

      while (added < found && added < count && ln != NULL) {
        if (replyWithDetail(c, ln->obj, detail_field)) {
          added++;
        } else { --found; }
        ln = ln->backward;
      }
    }
    else {
      ln = offset == 0 ? zsl->header->level[0].forward : zslGetElementByRank(zsl, offset+1);

      while (added < found && added < count && ln != NULL) {
        if (replyWithDetail(c, ln->obj, detail_field)) {
          added++;
        }
        else { --found; }
        ln = ln->level[0].forward;
      }
    }
  }

  decrRefCount(dstobj);
  data->found = found;
  data->added = added;
}


// Used if the smallest filter has a relatively big size compared to zset's size
void vfindByZWithFilters(redisClient *c, vfindData *data) {
  int desc = data->desc;
  int found = 0;
  int added = 0;
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  long filter_count = data->filter_count;
  long offset = data->offset;
  long count = data->count;
  long up_to = data->up_to;
  dict **allows = data->allows;
  dict **blocks = data->blocks;
  dict **filters = data->filters;
  robj *detail_field = data->detail_field;
  zskiplistNode *ln = data->ln;
  robj *item;

  while(ln != NULL) {
    item = ln->obj;

    for(int i = 0; i < filter_count; ++i) {
      if (!isMember(filters[i], item)) { goto next; }
    }
    item->blocked = heldback2(allow_count, allows, block_count, blocks, item);

    if (item->blocked && !data->include_blocked) {  goto next; }

    if (found++ >= offset && added < count) {
      if (replyWithDetail(c, item, detail_field)) {
        added++;
      }
      else { --found; }
    }

    if (added == count && found >= up_to) { break; }
    next:
    ln = desc ? ln->backward : ln->level[0].forward;
  }

  data->found = found;
  data->added = added;
}

static void initializeZsetIterator(vfindData *data) {
  zskiplist *zsl;
  zsl = data->zset->zsl;
  data->ln = data->desc ? zsl->tail : zsl->header->level[0].forward;
}
