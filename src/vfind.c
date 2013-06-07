#include "redis.h"
#include "viki.h"
#include <math.h>

#define VFIND_FILTER_START 13

typedef struct vfindData {
  int desc, found, added, include_blocked;
  long filter_count, offset, count, up_to;
  robj *detail_field;
  robj **filter_objects;
  robj *inclusion_list;
  dict *cap, *anti_cap, *exclusion_list, **filters;
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

  /* 5 keys (zset, cap, anticap, incl, excl) in addition to filters key */
  num = atoi(argv[12]->ptr) + 5;
  /* Sanity check. Don't return any key if the command is going to
   * reply with syntax error. */
  if (num > (argc-8)) {
    *numkeys = 0;
    return NULL;
  }
  keys = zmalloc(sizeof(int)*num);
  // zset key position
  keys[0] = 1;
  // cap key position
  keys[1] = 2;
  // anticap key position
  keys[2] = 3;
  // incl key position
  keys[3] = 8;
  // excl key position
  keys[4] = 9;
  // filters key positions
  for (i = 5; i < num; ++i) {
    keys[i] = 8+i;
  }
  *numkeys = num;
  return keys;
}

// Prepares all the sets(data, cap, anticap, filters) and calls vfindByFilters or vfindByZWithFilters
// depending on the ratio of the input set and the smallest filter set
// Syntax: vfind zset cap anticap offset count upto direction incl excl include_blocked detail_field filter_count [filter keys]
void vfindCommand(redisClient *c) {
  long filter_count;
  void *replylen;
  long offset, count, up_to;
  robj *items, *cap, *anti_cap;
  robj *inclusion_list, *exclusion_list;
  robj *direction, *include_blocked, *data_field;
  vfindData *data;

  // All the data checks
  if ((items = lookupKey(c->db, c->argv[1])) == NULL) {
    addReplyMultiBulkLen(c, 1);
    addReplyLongLong(c, 0);
    return;
  }
  if (checkType(c, items, REDIS_ZSET)) { return; }
  if ((cap = lookupKey(c->db, c->argv[2])) != NULL && checkType(c, cap, REDIS_SET)) { return; }
  if ((anti_cap = lookupKey(c->db, c->argv[3])) != NULL && checkType(c, anti_cap, REDIS_SET)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[4], &offset, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[5], &count, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[6], &up_to, NULL) != REDIS_OK)) { return; }
  direction = c->argv[7];
  if ((inclusion_list = lookupKey(c->db, c->argv[8])) != NULL && checkType(c, inclusion_list, REDIS_ZSET)) { return; }
  if ((exclusion_list = lookupKey(c->db, c->argv[9])) != NULL && checkType(c, exclusion_list, REDIS_SET)) { return; }
  include_blocked = c->argv[10];
  data_field = c->argv[11];
  if ((getLongFromObjectOrReply(c, c->argv[12], &filter_count, NULL) != REDIS_OK)) { return; }

  if (filter_count > (c->argc-12)) { addReply(c, shared.syntaxerr); return; }

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
  data->inclusion_list = inclusion_list;

  replylen = addDeferredMultiBulkLength(c);

  zsetConvert(items, REDIS_ENCODING_SKIPLIST);
  data->zset = items->ptr;
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;
  data->anti_cap = (anti_cap == NULL) ? NULL : (dict*)anti_cap->ptr;
  data->exclusion_list = (exclusion_list == NULL) ? NULL : (dict*)exclusion_list->ptr;

  if (!strcasecmp(direction->ptr, "asc")) { data->desc = 0; }
  // The keyword for blocked items is "withblocked"
  if (!strcasecmp(include_blocked->ptr, "withblocked")) { data->include_blocked = 1; }

  data->filter_count = filter_count;
  if (filter_count != 0) {
    data->filter_objects = zmalloc(sizeof(robj*) * filter_count);
    data->filters = zmalloc(sizeof(dict*) * filter_count);
    for(int i = 0; i < filter_count; ++i) {
      if ((data->filter_objects[i] = lookupKey(c->db, c->argv[i+VFIND_FILTER_START])) == NULL || checkType(c, data->filter_objects[i], REDIS_SET)) { goto reply; }
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
  setDeferredMultiBulkLength(c, replylen, (data->added) * 2 +1);
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
  long filter_count = data->filter_count;
  int found = 0;
  int added = 0;
  zset *zset = data->zset;
  dict **filters = data->filters;
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;
  robj *inclusion_list = data->inclusion_list;
  dict *exclusion_list = data->exclusion_list;
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
    
    item->blocked = heldback(cap, anti_cap, inclusion_list, exclusion_list, item);
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
          replyWithMetadata(c, generateMetadataObject(ln->obj));
        } else { --found; }
        ln = ln->backward;
      }
    }
    else {
      ln = offset == 0 ? zsl->header->level[0].forward : zslGetElementByRank(zsl, offset+1);

      while (added < found && added < count && ln != NULL) {    
        if (replyWithDetail(c, ln->obj, detail_field)) { 
          added++;
          replyWithMetadata(c, generateMetadataObject(ln->obj));
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
  long filter_count = data->filter_count;
  long offset = data->offset;
  long count = data->count;
  long up_to = data->up_to;
  dict **filters = data->filters;
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;
  robj *detail_field = data->detail_field;
  robj *inclusion_list = data->inclusion_list;
  dict *exclusion_list = data->exclusion_list;
  zskiplistNode *ln = data->ln;
  robj *item;

  while(ln != NULL) {
    item = ln->obj;

    for(int i = 0; i < filter_count; ++i) {
      if (!isMember(filters[i], item)) { goto next; }
    }
    item->blocked = heldback(cap, anti_cap, inclusion_list, exclusion_list, item);

    if (item->blocked && !data->include_blocked) {  goto next; }

    if (found++ >= offset && added < count) {
      if (replyWithDetail(c, item, detail_field)) { 
        added++; 
        replyWithMetadata(c, generateMetadataObject(item));
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
