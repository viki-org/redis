#include "redis.h"

typedef struct vsortData {
  int added;
  long count;
  robj *detail_field;
  dict *cap, *anti_cap;
} vsortData;

void vsortCommand(redisClient *c) {
  long count;
  void *replylen;
  robj *cap, *anti_cap;
  vsortData *data;

  if ((getLongFromObjectOrReply(c, c->argv[3], &count, NULL) != REDIS_OK)) { return; }
  if ((cap = lookupKey(c->db, c->argv[1])) != NULL && checkType(c, cap, REDIS_SET)) { return; }
  if ((anti_cap = lookupKey(c->db, c->argv[2])) != NULL && checkType(c, anti_cap, REDIS_SET)) { return; }

  data = zmalloc(sizeof(*data));
  data->detail_field = createStringObject("details", 7);
  data->added = 0;
  data->count = count;

  replylen = addDeferredMultiBulkLength(c);
  data->cap = (cap == NULL) ? NULL : (dict*)cap->ptr;
  data->anti_cap = (anti_cap == NULL) ? NULL : (dict*)anti_cap->ptr;

  setDeferredMultiBulkLength(c, replylen, data->added);
  decrRefCount(data->detail_field);
  zfree(data);
}
