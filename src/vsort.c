#include "redis.h"
#include "viki.h"

typedef struct vsortData {
  int added, id_offset;
  long allow_count, block_count, id_count, count;
  dict **allows, **blocks;
  robj *meta_field, *zscores;
} vsortData;

typedef struct {
  robj *item;
  double score;
} itemScore;


void vsortByViews(redisClient *c, vsortData *data);
void vsortByNone(redisClient *c, vsortData *data);
double getScore(robj *zsetObj, robj *item);
int itemScoreComparitor (const void* lhs, const void* rhs);


// count zset allow_count [allows] block_count [blocks] resource_count [resources]
void vsortCommand(redisClient *c) {
  int block_offset;
  long allow_count, block_count, count;
  void *replylen;
  robj *zscores;
  vsortData *data;

  if ((getLongFromObjectOrReply(c, c->argv[1], &count, NULL) != REDIS_OK)) { return; }
  if ((zscores = lookupKey(c->db, c->argv[2])) != NULL && checkType(c, zscores, REDIS_ZSET)) { return; }

  if ((getLongFromObjectOrReply(c, c->argv[3], &allow_count, NULL) != REDIS_OK)) { return; }
  block_offset = 4 + allow_count;
  if ((getLongFromObjectOrReply(c, c->argv[block_offset], &block_count, NULL) != REDIS_OK)) { return; }

  data = zmalloc(sizeof(*data));
  data->meta_field = createStringObject("meta", 4);
  data->added = 0;
  data->count = count;
  data->zscores = zscores;
  data->id_offset = 6 + allow_count + block_count;

  data->allows = loadSetArray(c, 4, &allow_count);
  data->allow_count = allow_count;

  data->blocks = loadSetArray(c, block_offset+1, &block_count);
  data->block_count = block_count;

  replylen = addDeferredMultiBulkLength(c);
  if (count < c->argc - data->id_offset) {
    vsortByViews(c, data);
  } else {
    vsortByNone(c, data);
  }
  setDeferredMultiBulkLength(c, replylen, data->added);
  decrRefCount(data->meta_field);
  if (data->allows != NULL) { zfree(data->allows); }
  if (data->blocks != NULL) { zfree(data->blocks); }
  zfree(data);
}

void vsortByViews(redisClient *c, vsortData *data) {
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  long count = data->count;
  int found = 0, lowest_at = 0;
  double lowest = -1;
  double scores[count];
  robj *zscores = data->zscores;
  dict **allows = data->allows;
  dict **blocks = data->blocks;
  robj **items = zmalloc(sizeof(robj*) * count);

  for(int i = data->id_offset; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback2(allow_count, allows, block_count, blocks, item)) { continue; }
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
  long allow_count = data->allow_count;
  long block_count = data->block_count;
  dict **allows = data->allows;
  dict **blocks = data->blocks;

  for(int i = data->id_offset; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback2(allow_count, allows, block_count, blocks, item)) { continue; }
    if (replyWithDetail(c, item, data->meta_field)) {
      ++(data->added);
    }
  }
}

int itemScoreComparitor (const void* lhs, const void* rhs) {
  return (*(itemScore*)rhs).score - (*(itemScore*)lhs).score;
}
