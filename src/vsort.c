#include "redis.h"
#include "viki.h"
#define VSORT_ID_START 7

typedef struct vsortData {
  int added;
  long count;
  dict *cap, *anti_cap;
  robj *meta_field, *zscores, *inclusion_list, *exclusion_list;
} vsortData;

typedef struct {
  robj *item;
  double score;
} itemScore;


void vsortByViews(redisClient *c, vsortData *data);
void vsortByNone(redisClient *c, vsortData *data);
double getScore(robj *zsetObj, robj *item);
int itemScoreComparitor (const void* lhs, const void* rhs);

void vsortCommand(redisClient *c) {
  int COLLECTION_TYPES[] = {REDIS_ZSET, REDIS_SET};
  long count;
  void *replylen;
  robj *cap, *anti_cap, *zscores, *inclusion_list, *exclusion_list;
  vsortData *data;

  if ((getLongFromObjectOrReply(c, c->argv[3], &count, NULL) != REDIS_OK)) { return; }
  if ((cap = lookupKey(c->db, c->argv[1])) != NULL && checkType(c, cap, REDIS_SET)) { return; }
  if ((anti_cap = lookupKey(c->db, c->argv[2])) != NULL && checkType(c, anti_cap, REDIS_SET)) { return; }
  if ((zscores = lookupKey(c->db, c->argv[4])) != NULL && checkType(c, zscores, REDIS_ZSET)) { return; }
  if ((inclusion_list = lookupKey(c->db, c->argv[5])) != NULL && checkTypes(c, inclusion_list, COLLECTION_TYPES)) { return; }
  if ((exclusion_list = lookupKey(c->db, c->argv[6])) != NULL && checkTypes(c, exclusion_list, COLLECTION_TYPES)) { return; }

  data = zmalloc(sizeof(*data));
  data->meta_field = createStringObject("meta", 4);
  data->added = 0;
  data->count = count;
  data->zscores = zscores;
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;
  data->anti_cap = (anti_cap == NULL) ? NULL : (dict*)anti_cap->ptr;
  data->inclusion_list = inclusion_list;
  data->exclusion_list = exclusion_list;

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
  robj *inclusion_list = data->inclusion_list;
  robj *exclusion_list = data->exclusion_list;
  long count = data->count;
  int found = 0, lowest_at = 0;
  double lowest = -1;
  double scores[count];
  robj **items = zmalloc(sizeof(robj*) * count);

  for(int i = VSORT_ID_START; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback(cap, anti_cap, inclusion_list, exclusion_list, item)) { continue; }
    double score = getScore(zscores, item);
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
  robj *inclusion_list = data->inclusion_list;
  robj *exclusion_list = data->exclusion_list;

  for(int i = VSORT_ID_START; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback(cap, anti_cap, inclusion_list, exclusion_list, item)) { continue; }
    if (replyWithDetail(c, item, data->meta_field)) {
      ++(data->added);
    }
  }
}

int itemScoreComparitor (const void* lhs, const void* rhs)
{
  return (*(itemScore*)rhs).score - (*(itemScore*)lhs).score;
}
