#include "redis.h"
#include "viki.h"

typedef struct vsortData {
  int added;
  long count;
  robj *detail_field, *country;
  dict *cap, *anti_cap;
} vsortData;

void vsort(redisClient *c, vsortData *data);
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
  data->detail_field = createStringObject("details", 7);
  data->added = 0;
  data->count = count;
  data->country = c->argv[4];
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;
  data->anti_cap = (anti_cap == NULL) ? NULL : (dict*)anti_cap->ptr;

  replylen = addDeferredMultiBulkLength(c);
  vsort(c, data);
  setDeferredMultiBulkLength(c, replylen, data->added);
  decrRefCount(data->detail_field);
  zfree(data);
}

void vsort(redisClient *c, vsortData *data) {
  dict *cap = data->cap;
  dict *anti_cap = data->anti_cap;
  robj *country = data->country;

  for(int i = 5; i < c->argc; ++i) {
    robj *item = c->argv[i];
    if (heldback(cap, anti_cap, item)) { continue; }
    long score = getViews(c, item, country);
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