#include "redis.h"
#include "viki.h"

typedef struct vsortData {
  int added;
  long count;
  robj *meta_field, *country;
  dict *cap, *anti_cap;
} vsortData;

void vsortByViews(redisClient *c, vsortData *data);
void vsortByNone(redisClient *c, vsortData *data);
long getViews(redisClient *c, robj *item, robj *country);

void vsortCommand(redisClient *c) {
  long count;
  void *replylen;
  robj *cap, *anti_cap;
  vsortData *data;

  if ((getLongFromObjectOrReply(c, c->argv[3], &count, NULL) != REDIS_OK)) { return; }
  if (strlen(c->argv[4]->ptr) != 2) { addReplyError(c, "value is out of range"); return; }
  if ((cap = lookupKey(c->db, c->argv[1])) != NULL && checkType(c, cap, REDIS_SET)) { return; }
  if ((anti_cap = lookupKey(c->db, c->argv[2])) != NULL && checkType(c, anti_cap, REDIS_SET)) { return; }

  data = zmalloc(sizeof(*data));
  data->meta_field = createStringObject("meta", 4);
  data->added = 0;
  data->count = count;
  data->country = c->argv[4];
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;
  data->anti_cap = (anti_cap == NULL) ? NULL : (dict*)anti_cap->ptr;

  replylen = addDeferredMultiBulkLength(c);
  if (count < c->argc - 5) {
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
  robj *country = data->country;
  long count = data->count;
  int found = 0, lowest = -1, lowest_at = 0;
  int scores[count];
  robj **items = zmalloc(sizeof(robj*) * count);

  for(int i = 5; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback(cap, anti_cap, item)) { continue; }
    long score = getViews(c, item, country);
    if (found < count) {
      items[found] = item;
      if (lowest == -1 || score < lowest) {
        lowest = score;
        lowest_at = found;
      }
      scores[found++] = score;
    } else if (score > lowest) {
      items[lowest_at] = item;
      lowest = score;
      for(int j = 0; j < count; ++j) {
        if (scores[j] < lowest) {
          lowest = scores[j];
          lowest_at = j;
        }
      }
    }
  }
  for(int i = 0; i < found; ++i) {
    if (replyWithDetail(c, items[i], data->meta_field)) {
      ++(data->added);
    }
  }
  zfree(items);
}

void vsortByNone(redisClient *c, vsortData *data) {
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;

  for(int i = 5; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback(cap, anti_cap, item)) { continue; }
    if (replyWithDetail(c, item, data->meta_field)) {
      ++(data->added);
    }
  }
}

long getViews(redisClient *c, robj *item, robj *country) {
  int key_length = strlen(item->ptr);
  robj *key = createStringObject(NULL, key_length + 15);
  char *k = key->ptr;
  memcpy(k, "r:", 2);
  memcpy(k + 2, item->ptr, key_length);
  memcpy(k + 2 + key_length, ":views:recent", 13);
  robj *value = getHashValue(c, key, country);
  decrRefCount(key);
  if (value == NULL) {
    return -1;
  }
  long score = (long)value->ptr;
  decrRefCount(value);
  return score;
}