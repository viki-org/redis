#include "redis.h"
#include <math.h>

typedef struct vfindData {
  int desc, found, added;
  long filter_count, offset, count;
  robj *summary_field;
  robj **filter_objects;
  dict *cap, *anti_cap, **filters;
  zskiplistNode *ln;
  zset *zset;
} vfindData;

int qsortCompareSetsByCardinality(const void *s1, const void *s2);
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);

void vfindByZWithFilters(redisClient *c, vfindData *data);
void vfindByFilters(redisClient *c, vfindData *data);

static void initializeZsetIterator(vfindData *data);
static int replyWithSummary(redisClient *c, robj *item, robj *summary_field);
static int heldback(dict *cap, dict *anti_cap, robj *item);
static int isMember(dict *subject, robj *item);

int *vfindGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
  int i, num, *keys;
  REDIS_NOTUSED(cmd);
  REDIS_NOTUSED(flags);

  num = atoi(argv[4]->ptr) + 3;
  /* Sanity check. Don't return any key if the command is going to
   * reply with syntax error. */
  if (num > (argc-8)) {
    *numkeys = 0;
    return NULL;
  }
  keys = zmalloc(sizeof(int)*num);
  keys[0] = 1;
  keys[1] = 2;
  keys[2] = 3;
  for (i = 3; i < num; i++) {
    keys[i] = 3+i;
  }
  *numkeys = num;
  return keys;
}

void vfindCommand(redisClient *c) {
  long filter_count;
  void *replylen;
  long offset;
  long count;
  robj *zobj, *cap, *anti_cap;
  vfindData *data;

  if ((getLongFromObjectOrReply(c, c->argv[4], &filter_count, NULL) != REDIS_OK)) { return; }
  if (filter_count > (c->argc-8)) {
    addReply(c,shared.syntaxerr);
    return;
  }
  if ((getLongFromObjectOrReply(c, c->argv[6 + filter_count], &offset, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[7 + filter_count], &count, NULL) != REDIS_OK)) { return; }

  data = zmalloc(sizeof(*data));
  data->summary_field = createStringObject("summary", 7);
  data->filters = NULL;
  data->filter_objects = NULL;
  data->offset = offset;
  data->count = count;
  data->added = 0;
  data->found = 0;
  data->desc = 1;

  replylen = addDeferredMultiBulkLength(c);
  if ((zobj = lookupKey(c->db, c->argv[1])) == NULL || zobj->type != REDIS_ZSET) { goto reply; }
  if ((cap = lookupKey(c->db, c->argv[2])) != NULL && checkType(c, cap, REDIS_SET)) { (data->added)++; goto reply; }
  if ((anti_cap = lookupKey(c->db, c->argv[3])) != NULL && checkType(c, anti_cap, REDIS_SET)) { (data->added)++; goto reply; }

  zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);
  data->zset = zobj->ptr;
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;
  data->anti_cap = (anti_cap == NULL) ? NULL : (dict*)anti_cap->ptr;

  if (!strcasecmp(c->argv[5 + filter_count]->ptr, "asc")) { data->desc = 0; }

  data->filter_count = filter_count;
  if (filter_count != 0) {
    data->filter_objects = zmalloc(sizeof(robj*) * filter_count);
    data->filters = zmalloc(sizeof(dict*) * filter_count);
    for(int i = 0; i < filter_count; ++i) {
      if ((data->filter_objects[i] = lookupKey(c->db, c->argv[i+5])) == NULL || checkType(c, data->filter_objects[i], REDIS_SET)) { goto reply; }
    }
    qsort(data->filter_objects, filter_count, sizeof(robj*), qsortCompareSetsByCardinality);
    for (int i = 0; i < filter_count; ++i) {
      data->filters[i] = (dict*)data->filter_objects[i]->ptr;
    }
    int size = dictSize(data->filters[0]);
    int ratio = zsetLength(zobj) / size;

    if ((size < 100 && ratio > 1) || (size < 500 && ratio > 2) || (size < 2000 && ratio > 3)) {
      vfindByFilters(c, data);
      goto reply;
    }
  }
  initializeZsetIterator(data);
  vfindByZWithFilters(c, data);

reply:
  addReplyLongLong(c, data->found);
  setDeferredMultiBulkLength(c, replylen, (data->added)+1);
  if (data->filters != NULL) { zfree(data->filters); }
  if (data->filter_objects != NULL) { zfree(data->filter_objects); }
  decrRefCount(data->summary_field);
  zfree(data);
}

void vfindByFilters(redisClient *c, vfindData *data) {
  zset *dstzset;
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
  dict **filters = data->filters;
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;
  robj *summary_field = data->summary_field;
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
    if (heldback(cap, anti_cap, item)) { goto next; }
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

  dictReleaseIterator(si->di);
  zfree(si);

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

  decrRefCount(dstobj);
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
  dict **filters = data->filters;
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;
  robj *summary_field = data->summary_field;
  zskiplistNode *ln = data->ln;
  robj *item;


  while(ln != NULL) {
    item = ln->obj;
    for(int i = 0; i < filter_count; ++i) {
      if (!isMember(filters[i], item)) { goto next; }
    }
    if (heldback(cap, anti_cap, item)) { goto next; }
    if (found++ >= offset && added < count) {
      if (replyWithSummary(c, item, summary_field)) { added++; }
      else { --found; }
    }
    if (found > 1000 && added == count) { break; }
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
inline static int heldback(dict *cap, dict *anti_cap, robj *item) {
  if (cap == NULL || !isMember(cap, item)) { return 0; }
  return (anti_cap == NULL || !isMember(anti_cap, item));
}

inline static int isMember(dict *subject, robj *item) {
  return dictFind(subject, item) != NULL;
}