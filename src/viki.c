#include "redis.h"
#include "viki.h"
#include <stdio.h>

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
  robj *key = generateKey(item);
  robj *value = getHashValue(c, key, field);

  sds key_str = item->ptr;
  if (key_str[sdslen(key_str) - 1] == 'b') {
    robj *extended_value = mergeBrickResourceDetails(c, value, key, field);
    if (extended_value != value){
      decrRefCount(value);
      value = extended_value;
    }
  }

  decrRefCount(key);
  return value;
}

robj *getHashValue(redisClient *c, robj *key, robj *field) {
  robj *value = NULL;
  robj *o = lookupKey(c->db, key);
  if (o != NULL) { value = hashTypeGetObject(o, field); }
  return value;
}

robj *generateKey(robj *item) {
  int key_length = strlen(item->ptr);
  robj *key = createStringObject(NULL, key_length + 2);
  char *k = key->ptr;
  memcpy(k, "r:", 2);
  memcpy(k + 2, item->ptr, key_length);
  return key;
}

robj *mergeBrickResourceDetails(redisClient *c, robj *brick, robj *key, robj *field) {
  robj *resource_field = createStringObject("resource_id", 11);
  robj *resource_id = getHashValue(c, key, resource_field);
  decrRefCount(resource_field);
  if (resource_id == NULL) { return brick; }

  robj *resource_key = generateKey(resource_id);
  decrRefCount(resource_id);
  robj *resource = getHashValue(c, resource_key, field);
  decrRefCount(resource_key);
  if (resource == NULL) { return brick; }

  int brick_length = strlen(brick->ptr);
  int resource_length = strlen(resource->ptr);
  robj *data = createStringObject(NULL, brick_length + resource_length + 14);
  char *p = data->ptr;
  memcpy(p, brick->ptr, brick_length);
  memcpy(p + brick_length - 1, ", \"resource\": ", 14);
  memcpy(p + brick_length + 13, resource->ptr, resource_length);
  memcpy(p + brick_length + 13 + resource_length, "}", 1);

  decrRefCount(resource);
  return data;
}
