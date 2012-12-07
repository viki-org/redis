#include "redis.h"
#include <math.h>


typedef struct vfindData {
  int desc, found, added;
  long filter_count, offset, count;
  robj **filters, *cap, *summary_field;
  zskiplistNode *ln;
  zset *zset;
} vfindData;

int qsortCompareSetsByCardinality(const void *s1, const void *s2);
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);

void vfindByZWithFilters(redisClient *c, vfindData *data);
void vfindWithoutFilters(redisClient *c, vfindData *data);
void vfindByFilters(redisClient *c, vfindData *data);

static void initializeZsetIterator(vfindData *data);
static int replyWithSummary(redisClient *c, robj *item, robj *summary_field);
static int heldback(robj *cap, robj *item);

int *vfindGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
  int i, num, *keys;
  REDIS_NOTUSED(cmd);
  REDIS_NOTUSED(flags);

  num = atoi(argv[3]->ptr) + 2;
  /* Sanity check. Don't return any key if the command is going to
   * reply with syntax error. */
  if (num > (argc-4)) {
    *numkeys = 0;
    return NULL;
  }
  keys = zmalloc(sizeof(int)*num);
  keys[0] = 1;
  keys[1] = 2;
  for (i = 2; i < num; i++) {
    keys[i] = 2+i;
  }
  *numkeys = num;
  return keys;
}

void vfindCommand(redisClient *c) {
  long filter_count;
  void *replylen = addDeferredMultiBulkLength(c);
  robj *zobj;

  vfindData *data = zmalloc(sizeof(*data));
  data->summary_field = createStringObject("summary", 7);
  data->filters = NULL;
  data->added = 0;
  data->found = 0;

  if ((getLongFromObjectOrReply(c, c->argv[3], &filter_count, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[5 + filter_count], &data->offset, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[6 + filter_count], &data->count, NULL) != REDIS_OK)) { return; }
  if ((zobj = lookupKey(c->db, c->argv[1])) == NULL || checkType(c, zobj , REDIS_ZSET)) { goto cleanup; }
  zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);
  data->zset = zobj->ptr;

  data->desc = 1;
  if (!strcasecmp(c->argv[4 + filter_count]->ptr, "asc")) { data->desc = 0; }
  data->cap = lookupKey(c->db, c->argv[2]);

  if (filter_count == 0) {
    initializeZsetIterator(data);
    vfindWithoutFilters(c, data);
    goto cleanup;
  }

  data->filter_count = filter_count;
  data->filters = zmalloc(sizeof(robj*) * filter_count);
  for(int i = 0; i < filter_count; i++) {
    if ((data->filters[i] = lookupKey(c->db, c->argv[i+4])) == NULL || checkType(c, data->filters[i], REDIS_SET)) { goto cleanup; }
  }
  qsort(data->filters,filter_count, sizeof(robj*), qsortCompareSetsByCardinality);

  if (zsetLength(zobj) / setTypeSize(data->filters[0]) > 1) {
    vfindByFilters(c, data);
  }
  else {
    initializeZsetIterator(data);
    vfindByZWithFilters(c, data);
  }

cleanup:
  addReplyLongLong(c, data->found);
  setDeferredMultiBulkLength(c, replylen, (data->added)+1);
  if (data->filters != NULL) { zfree(data->filters); }
  decrRefCount(data->summary_field);
  zfree(data);
}

void vfindByFilters(redisClient *c, vfindData *data) {
  zset *dstzset;
  setTypeIterator *si;
  robj *item;
  zskiplist *zsl;
  zskiplistNode *ln;

  int desc = data->desc;
  long offset = data->offset;
  long count = data->count;
  long filter_count = data->filter_count;
  int found = 0;
  int added = 0;
  zset *zset = data->zset;
  robj **filters = data->filters;
  robj *cap = data->cap;
  robj *summary_field = data->summary_field;
  int64_t intobj;
  double score;

  robj *dstobj = createZsetObject();
  dstzset = dstobj->ptr;
  zsl = dstzset->zsl;

  si = setTypeInitIterator(filters[0]);
  while((setTypeNext(si, &item, &intobj)) != -1) {
    for(int i = 1; i < filter_count; ++i) {
      if (!setTypeIsMember(filters[i], item)) { goto next; }
    }
    if (heldback(cap, item)) { goto next; }
    dictEntry *de;
    if ((de = dictFind(zset->dict,item)) != NULL) {
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

  if (found > offset) {
    if (offset == 0) {
      ln = desc ? zsl->tail : zsl->header->level[0].forward;
    } else {
      ln = desc ? zslGetElementByRank(zsl, found - offset) : zslGetElementByRank(zsl, offset+1);
    }

    while (added < found && added < count && ln != NULL) {
      if (replyWithSummary(c, ln->obj, summary_field)) { added++; }
      else { --found; }
      ln = desc ? ln->backward : ln->level[0].forward;
    }
  }

  setTypeReleaseIterator(si);
  decrRefCount(dstobj);

  data->found = found;
  data->added = added;
}
void vfindWithoutFilters(redisClient *c, vfindData *data) {
  int desc = data->desc;
  long offset = data->offset;
  long count = data->count;
  int found = 0;
  int added = 0;
  robj *cap = data->cap;
  robj *summary_field = data->summary_field;
  zskiplistNode *ln = data->ln;
  robj *item;

  while(ln != NULL) {
    item = ln->obj;
    if (heldback(cap, item)) { goto next; }
    if (found++ >= offset && added < count) {
      if (replyWithSummary(c, item, summary_field)) { added++; }
      else { --found; }
    }
    if (found > 500 && added == count) { break; }
next:
    ln = desc ? ln->backward : ln->level[0].forward;
  }
  data->found = found;
  data->added = added;
}

void vfindByZWithFilters(redisClient *c, vfindData *data) {
  int desc = data->desc;
  int found = 0;
  int added = 0;
  long filter_count = data->filter_count;
  long offset = data->offset;
  long count = data->count;
  robj **filters = data->filters;
  robj *cap = data->cap;
  robj *summary_field = data->summary_field;
  zskiplistNode *ln = data->ln;
  robj *item;


  while(ln != NULL) {
    item = ln->obj;
    for(int i = 0; i < filter_count; ++i) {
      if (!setTypeIsMember(filters[i], item)) { goto next; }
    }
    if (heldback(cap, item)) { goto next; }
    if (found++ >= offset && added < count) {
      if (replyWithSummary(c, item, summary_field)) { added++; }
      else { --found; }
    }
    if (found > 500 && added == count) { break; }
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

static int replyWithSummary(redisClient *c, robj *item, robj *summary_field) {
  robj *o, *hash;
  char *k;
  int r = 1, field_length;

  field_length = strlen(item->ptr);
  hash = createStringObject(NULL, field_length + 2);
  k = hash->ptr;
  memcpy(k, "r:", 2);
  memcpy(k + 2, item->ptr, field_length);
  o = lookupKey(c->db, hash);
  if (o != NULL) {
    o = hashTypeGetObject(o, summary_field);
    addReplyBulk(c, o);
    decrRefCount(o);
  } else {
    r = 0;
  }
  decrRefCount(hash);
  return r;
}
inline static int heldback(robj *cap, robj *item) {
  return (cap != NULL && setTypeIsMember(cap, item));
}
