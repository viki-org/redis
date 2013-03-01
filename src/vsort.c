#include "redis.h"
#include "viki.h"
#define VSORT_ID_START 5

typedef struct vsortData {
  int added;
  long count;
  robj *meta_field, *zscores;
  dict *cap, *anti_cap;
} vsortData;

typedef struct
{
  robj *item;
  double score;
} itemScore;


void vsortByViews(redisClient *c, vsortData *data);
void vsortByNone(redisClient *c, vsortData *data);
double getScore(robj *item, robj *zscores);
int itemScoreComparitor (const void* lhs, const void* rhs);

void vsortCommand(redisClient *c) {
  long count;
  void *replylen;
  robj *cap, *anti_cap, *zscores;
  vsortData *data;

  if ((getLongFromObjectOrReply(c, c->argv[3], &count, NULL) != REDIS_OK)) { return; }
  if ((cap = lookupKey(c->db, c->argv[1])) != NULL && checkType(c, cap, REDIS_SET)) { return; }
  if ((anti_cap = lookupKey(c->db, c->argv[2])) != NULL && checkType(c, anti_cap, REDIS_SET)) { return; }
  if ((zscores = lookupKey(c->db, c->argv[4])) != NULL && checkType(c, zscores, REDIS_ZSET)) { return; }

  data = zmalloc(sizeof(*data));
  data->meta_field = createStringObject("meta", 4);
  data->added = 0;
  data->count = count;
  data->zscores = zscores;
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;
  data->anti_cap = (anti_cap == NULL) ? NULL : (dict*)anti_cap->ptr;

  replylen = addDeferredMultiBulkLength(c);
  if (count < c->argc - VSORT_ID_START) {
    vsortByViews(c, data);
  } else {
    vsortByNone(c, data);
  }
  setDeferredMultiBulkLength(c, replylen, data->added);
  decrRefCount(data->meta_field);
  zfree(data);
}

void vsortByViews(redisClient *c, vsortData *data) {
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;
  robj *zscores = data->zscores;
  long count = data->count;
  int found = 0, lowest_at = 0;
  double lowest = -1;
  double scores[count];
  robj **items = zmalloc(sizeof(robj*) * count);

  for(int i = VSORT_ID_START; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback(cap, anti_cap, item)) { continue; }
    double score = getScore(item, zscores);
    if (found < count) {
      items[found] = item;
      if (lowest == -1 || score <= lowest) { //it's better to replace later items with the same score, as the input might be sorted some way (it is!)
        lowest = score;
        lowest_at = found;
      }
      scores[found++] = score;
    } else if (score > lowest) {
      items[lowest_at] = item;
      scores[lowest_at] = score;
      lowest = scores[0];
      lowest_at = 0;
      for(int j = 1; j < count; ++j) {
        if (scores[j] <= lowest) {
          lowest = scores[j];
          lowest_at = j;
        }
      }
    }
  }

  if (found > 1) {
    itemScore map[found];
    for (int i = 0 ; i < found; ++i) {
      map[i].item = items[i];
      map[i].score = scores[i];
    }
    qsort(map, found, sizeof(itemScore), itemScoreComparitor);
    for(int i = 0; i < found; ++i) {
      if (replyWithDetail(c, map[i].item, data->meta_field)) {
        ++(data->added);
      }
    }
  } else if (found == 1) {
    if (replyWithDetail(c, items[0], data->meta_field)) {
      ++(data->added);
    }
  }
  zfree(items);
}

void vsortByNone(redisClient *c, vsortData *data) {
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;

  for(int i = VSORT_ID_START; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback(cap, anti_cap, item)) { continue; }
    if (replyWithDetail(c, item, data->meta_field)) {
      ++(data->added);
    }
  }
}

double getScore(robj *item, robj *zscores) {
  double score;

  if (zscores == NULL) { return 0; }

  if (zscores->encoding == REDIS_ENCODING_ZIPLIST) {
    return zzlFind(zscores->ptr, item, &score) == NULL ? 0 : score;
  }
  zset *zs = zscores->ptr;
  dictEntry *de = dictFind(zs->dict, item);
  return de == NULL ? 0 : *(double*)dictGetVal(de);
}

int itemScoreComparitor (const void* lhs, const void* rhs)
{
  return (*(itemScore*)rhs).score - (*(itemScore*)lhs).score;
}
