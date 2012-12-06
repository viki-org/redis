#include "redis.h"
#include <math.h>

int *vfindGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
  int i, num, *keys;
  REDIS_NOTUSED(cmd);
  REDIS_NOTUSED(flags);

  num = atoi(argv[3]->ptr) + 2;
  /* Sanity check. Don't return any key if the command is going to
   * reply with syntax error. */
  if (num > (argc-4)) {
    *numkeys = 0;
    return NULL;
  }
  keys = zmalloc(sizeof(int)*num);
  keys[0] = 1;
  keys[1] = 2;
  for (i = 2; i < num; i++) {
    keys[i] = 2+i;
  }
  *numkeys = num;
  return keys;
}

void vfindCommand(redisClient *c) {
  int skip, field_length, found = 0, added = 0, desc = 1;
  long offset, count, filter_count;
  void *replylen = NULL;
  char *k;
  robj *cap, *o, *item, **filters, *hash, *zobj;
  zset *zset;
  zskiplist *zsl;
  zskiplistNode *ln;
  redisDb *db = c->db;
  robj *summary_field = createStringObject("summary", 7);

  replylen = addDeferredMultiBulkLength(c);

  if ((getLongFromObjectOrReply(c, c->argv[3], &filter_count, NULL) != REDIS_OK)) { goto end; }
  filters = zmalloc(sizeof(robj*) * filter_count);

  if ((zobj = lookupKey(db, c->argv[1])) == NULL || checkType(c, zobj, REDIS_ZSET)) { goto end; }
  zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);
  zset = zobj->ptr;
  zsl = zset->zsl;
  ln = zsl->tail;

  cap = lookupKey(db, c->argv[2]);

  for(int i = 0; i < filter_count; i++) {
    if ((filters[i] = lookupKey(db, c->argv[i+4])) == NULL || checkType(c, filters[i], REDIS_SET)) { goto end; }
  }

  if (!strcasecmp(c->argv[4 + filter_count]->ptr, "asc")) {
    desc = 0;
    ln = zsl->header->level[0].forward;
  }
  if((getLongFromObjectOrReply(c, c->argv[5 + filter_count], &offset, NULL) != REDIS_OK)) { return; }
  if((getLongFromObjectOrReply(c, c->argv[6 + filter_count], &count, NULL) != REDIS_OK)) { return; }


  while(ln != NULL) {
    item = ln->obj;
    skip = 0;
    for(int i = 0; i < filter_count; ++i) {
      if (!setTypeIsMember(filters[i], item)) {
        skip = 1;
        break;
      }
    }
    if (skip == 0 && (cap == NULL || !setTypeIsMember(cap, item))) {
      if (found++ >= offset && added < count) {
        field_length = strlen(item->ptr);
        hash = createStringObject(NULL, field_length + 2);
        k = hash->ptr;
        memcpy(k, "r:", 2);
        memcpy(k + 2, item->ptr, field_length);
        o = lookupKey(db, hash);
        if (o == NULL) {
          //There is no summary data for the element
          --found;
        } else {
          o = hashTypeGetObject(o, summary_field);
          addReplyBulk(c, o);
          decrRefCount(o);
          ++added;
        }
        decrRefCount(hash);
      }
    }
    if (found > 500 && added == count) { break; }

    ln = desc ? ln->backward : ln->level[0].forward;
  }

end:
  zfree(filters);
  decrRefCount(summary_field);
  addReplyLongLong(c, found);
  setDeferredMultiBulkLength(c, replylen, added+1);
}
