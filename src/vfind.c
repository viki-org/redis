#include "redis.h"
#include "viki.h"
#include <math.h>

typedef struct vfindData {
  int desc, found, added;
  long filter_count, offset, count, up_to, inclusion_count, exclusion_count;
  robj *detail_field;
  robj **filter_objects;
  robj **inclusion_lists, **exclusion_lists;
  dict **filters;
  zskiplistNode *ln;
  zset *zset;
} vfindData;


zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);

void vfindByZWithFilters(redisClient *c, vfindData *data);
void vfindByFilters(redisClient *c, vfindData *data);

static void initializeZsetIterator(vfindData *data);

int *vfindGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys, int flags) {
  int i, num, *keys;
  REDIS_NOTUSED(cmd);
  REDIS_NOTUSED(flags);

  int inclusion_count_pos = 7;
  int inclusion_count = atoi(argv[inclusion_count_pos]->ptr);

  int exclusion_count_pos = inclusion_count_pos + inclusion_count + 1;
  int exclusion_count = atoi(argv[exclusion_count_pos]->ptr);

  int filter_count_pos = exclusion_count_pos + exclusion_count + 1;
  int filter_count = atoi(argv[filter_count_pos]->ptr);

  num = inclusion_count + exclusion_count + filter_count + 1;
  /* Sanity check. Don't return any key if the command is going to
   * reply with syntax error. */
  if (num > (argc-9)) {
    *numkeys = 0;
    return NULL;
  }
  keys = zmalloc(sizeof(int)*num);

  int t = 0;
  keys[t] = 1;
  for (i = 1; i <= inclusion_count; ++i) {
    keys[++t] = inclusion_count_pos + i;
  }
  for (i = 1; i <= exclusion_count; ++i) {
    keys[++t] = exclusion_count_pos + i;
  }
  for (i = 1; i <= filter_count; i++) {
    keys[++t] = filter_count_pos + i;
  }

  *numkeys = num;
  return keys;
}

// Prepares all the sets(data, cap, anticap, filters) and calls vfindByFilters or vfindByZWithFilters
// depending on the ratio of the input set and the smallest filter set
// Syntax: vfind zset detail_field offset count upto direction #incl [incls] #excl[excls] #filter [filters]
void vfindCommand(redisClient *c) {
  int COLLECTION_TYPES[] = {REDIS_ZSET, REDIS_SET};
  long filter_count, inclusion_count, exclusion_count;
  int inclusion_count_pos, exclusion_count_pos, filter_count_pos;
  int incl_start, excl_start, filter_start;
  void *replylen;
  long offset, count, up_to;
  robj *items;
  robj *direction, *data_field;
  vfindData *data;

  // All the data checks
  if ((items = lookupKey(c->db, c->argv[1])) == NULL) {
    addReplyMultiBulkLen(c, 1);
    addReplyLongLong(c, 0);
    return;
  }
  if (checkType(c, items, REDIS_ZSET)) { return; }
  data_field = c->argv[2];
  if ((getLongFromObjectOrReply(c, c->argv[3], &offset, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[4], &count, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[5], &up_to, NULL) != REDIS_OK)) { return; }

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
  data->inclusion_lists = NULL;
  data->exclusion_lists = NULL;

  zsetConvert(items, REDIS_ENCODING_SKIPLIST);
  data->zset = items->ptr;

  direction = c->argv[6];
  if (!strcasecmp(direction->ptr, "asc")) { data->desc = 0; }

  inclusion_count_pos = 7;
  if ((getLongFromObjectOrReply(c, c->argv[inclusion_count_pos], &inclusion_count, NULL) != REDIS_OK)) { goto cleanup; }
  // if (inclusion_count > (c->argc-8)) { addReply(c, shared.syntaxerr); goto cleanup; }
  incl_start = inclusion_count_pos + 1;
  data->inclusion_count = inclusion_count;
  if (inclusion_count != 0) {
    data->inclusion_lists = zmalloc(sizeof(robj*) * inclusion_count);
    for(int i = 0; i < inclusion_count; ++i) {
      if ((data->inclusion_lists[i] = lookupKey(c->db, c->argv[i+incl_start])) != NULL && checkTypes(c, data->inclusion_lists[i], COLLECTION_TYPES)) { goto cleanup; }
    }
  }

  exclusion_count_pos = incl_start + inclusion_count;
  if ((getLongFromObjectOrReply(c, c->argv[exclusion_count_pos], &exclusion_count, NULL) != REDIS_OK)) { goto cleanup; }
  // if (exclusion_count > (c->argc-9-inclusion_count)) { addReply(c, shared.syntaxerr); goto cleanup; }
  excl_start = exclusion_count_pos + 1;
  data->exclusion_count = exclusion_count;
  if (exclusion_count != 0) {
    data->exclusion_lists = zmalloc(sizeof(robj*) * exclusion_count);
    for (int i = 0; i < exclusion_count; ++i) {
      if ((data->exclusion_lists[i] = lookupKey(c->db, c->argv[i+excl_start])) != NULL && checkTypes(c, data->exclusion_lists[i], COLLECTION_TYPES)) { goto cleanup; }
    }
  }

  replylen = addDeferredMultiBulkLength(c);
  
  filter_count_pos = excl_start + exclusion_count;
  if ((getLongFromObjectOrReply(c, c->argv[filter_count_pos], &filter_count, NULL) != REDIS_OK)) { goto cleanup; }
  // if (filter_count > (c->argc-10-inclusion_count-exclusion_count)) { addReply(c, shared.syntaxerr); goto cleanup; }
  filter_start = filter_count_pos + 1;
  data->filter_count = filter_count;
  if (filter_count != 0) {
    data->filter_objects = zmalloc(sizeof(robj*) * filter_count);
    data->filters = zmalloc(sizeof(dict*) * filter_count);
    for(int i = 0; i < filter_count; ++i) {
      if ((data->filter_objects[i] = lookupKey(c->db, c->argv[i+filter_start])) == NULL || checkType(c, data->filter_objects[i], REDIS_SET)) { goto cleanup; }
    }
    qsort(data->filter_objects, filter_count, sizeof(robj*), qsortCompareSetsByCardinality);
    for (int i = 0; i < filter_count; ++i) {
      data->filters[i] = (dict*)data->filter_objects[i]->ptr;
    }
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
  setDeferredMultiBulkLength(c, replylen, (data->added) * 1 + 1);

  cleanup:
  if (data->filters != NULL) { zfree(data->filters); }
  if (data->filter_objects != NULL) { zfree(data->filter_objects); }
  if (data->inclusion_lists != NULL) { zfree(data->inclusion_lists); }
  if (data->exclusion_lists != NULL) { zfree(data->exclusion_lists); }
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
  robj **inclusion_lists = data->inclusion_lists;
  long inclusion_count = data->inclusion_count;
  robj **exclusion_lists = data->exclusion_lists;
  long exclusion_count = data->exclusion_count;
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
    if (heldback(inclusion_count, inclusion_lists, exclusion_count,exclusion_lists, item)) { goto next; }
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
        if (replyWithDetail(c, ln->obj, detail_field)) { added++; }
        else { --found; }
        ln = ln->backward;
      }
    }
    else {
      ln = offset == 0 ? zsl->header->level[0].forward : zslGetElementByRank(zsl, offset+1);

      while (added < found && added < count && ln != NULL) {
        if (replyWithDetail(c, ln->obj, detail_field)) { added++; }
        else { --found; }
        ln = ln->level[0].forward;
      }
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
  long up_to = data->up_to;
  dict **filters = data->filters;
  robj *detail_field = data->detail_field;
  robj **inclusion_lists = data->inclusion_lists;
  long inclusion_count = data->inclusion_count;
  robj **exclusion_lists = data->exclusion_lists;
  long exclusion_count = data->exclusion_count;
  zskiplistNode *ln = data->ln;
  robj *item;


  while(ln != NULL) {
    item = ln->obj;
    for(int i = 0; i < filter_count; ++i) {
      if (!isMember(filters[i], item)) { goto next; }
    }
    if (heldback(inclusion_count, inclusion_lists, exclusion_count,exclusion_lists, item)) { goto next; }
    if (found++ >= offset && added < count) {
      if (replyWithDetail(c, item, detail_field)) { added++; }
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
