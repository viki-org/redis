#include "redis.h"
#include "viki.h"

int replyWithDetail(redisClient *c, robj *item, robj *field) {
  robj *value = getResourceValue(c, item, field);
  if (value == NULL) {
    return 0;
  }
  addReplyBulk(c, value);
  decrRefCount(value);
  return 1;
}

robj *getResourceValue(redisClient *c, robj *item, robj *field) {
  int key_length = strlen(item->ptr);
  robj *key = createStringObject(NULL, key_length + 2);
  char *k = key->ptr;
  memcpy(k, "r:", 2);
  memcpy(k + 2, item->ptr, key_length);
  robj *value = getHashValue(c, key, field);
  decrRefCount(key);
  return value;
}

robj *getHashValue(redisClient *c, robj *key, robj *field) {
  robj *value = NULL;
  robj *o = lookupKey(c->db, key);
  if (o != NULL) {
    value = hashTypeGetObject(o, field);
  }
  return value;
}
