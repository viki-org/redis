#include "redis.h"
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);
void xdiffCommand(redisClient *c) {
  long offset, count, added = 0;
  robj *zobj, *sobj;
  zset *zset;
  dict *diff;
  void *replylen;

  if ((getLongFromObjectOrReply(c, c->argv[3], &offset, NULL) != REDIS_OK)) { return; }
  if ((getLongFromObjectOrReply(c, c->argv[4], &count, NULL) != REDIS_OK)) { return; }

  zobj = lookupKeyReadOrReply(c, c->argv[1], shared.czero);
  if (zobj == NULL || checkType(c, zobj, REDIS_ZSET)) { return; }

  sobj = lookupKeyReadOrReply(c, c->argv[2], shared.czero);
  if (sobj == NULL || checkType(c, sobj, REDIS_SET)) { return; }

  zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);
  zset = zobj->ptr;
  diff = (dict*)sobj->ptr;

  long zsetlen = dictSize(zset->dict);
  zskiplistNode *ln = zslGetElementByRank(zset->zsl, zsetlen - offset);

  replylen = addDeferredMultiBulkLength(c);
  while(ln != NULL) {
    robj *item = ln->obj;
    if (dictFind(diff, item) == NULL) {
      addReplyBulk(c, item);
      if (++added == count) { break; }
    }
    ln = ln->backward;
  }
  setDeferredMultiBulkLength(c, replylen, added);
}