#include "redis.h"
#include "viki.h"
#include <stdio.h>

int replyWithDetail(redisClient *c, robj *item) {
  robj *value = getResourceValue(c, item);
  if (value == NULL) {
    return 0;
  }
  addReplyBulk(c, value);
  decrRefCount(value);
  return 1;
}

robj *getResourceValue(redisClient *c, robj *item) {
  int id_length = strlen(item->ptr);
  int block_length = item->blocked == 1 ? 17 : 18; // '","blocked":true}'  or '","blocked":false}'
  robj *details = createStringObject(NULL, 7 + id_length + block_length);
  char *p = details->ptr;
  memcpy(p, "{\"id\":\"", 7);
  int position = 7;
  memcpy(p + position, item->ptr, id_length);
  position += id_length;
    if (item->blocked == 1) {
    memcpy(p + position, "\",\"blocked\":true}", 17);
  } else {
    memcpy(p + position, "\",\"blocked\":false}", 18);
  }
  return details;
}

double getScore(robj *zsetObj, robj *item) {
  if (zsetObj == NULL) { return -1; }
  if (zsetObj->encoding == REDIS_ENCODING_ZIPLIST) {
    double score;
    return zzlFind(zsetObj->ptr, item, &score) == NULL ? -1 : score;
  }
  zset *zs = zsetObj->ptr;
  dictEntry *de = dictFind(zs->dict, item);
  return de == NULL ? -1 : *(double*)dictGetVal(de);
}

dict **loadSetArray(redisClient *c, int offset, long *count) {
  long total = *count;
  if (total == 0) { return NULL; }

  long misses = 0;
  robj *set;
  dict **array = zmalloc(sizeof(dict*) * total);
  for(int i = 0; i < total; ++i) {
    if (((set = lookupKey(c->db, c->argv[i+offset])) == NULL) || set->type != REDIS_SET) {
      ++misses;
    } else {
      array[i-misses] = (dict*)set->ptr;
    }
  }
  *count = total - misses;
  return array;
}
