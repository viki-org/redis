#include "redis.h"
#include <math.h>

/* Store value retrieved from the iterator. */
typedef struct {
  int flags;
  unsigned char _buf[32]; /* Private buffer. */
  robj *ele;
  unsigned char *estr;
  unsigned int elen;
  long long ell;
  double score;
} zsetopval;

typedef struct {
  robj *subject;
  int type; /* Set, sorted set */
  int encoding;
  double weight;

  union {
    /* Set iterators. */
    union _iterset {
      struct {
        intset *is;
        int ii;
      } is;
      struct {
        dict *dict;
        dictIterator *di;
        dictEntry *de;
      } ht;
    } set;

    /* Sorted set iterators. */
    union _iterzset {
      struct {
        unsigned char *zl;
        unsigned char *eptr, *sptr;
      } zl;
      struct {
        zset *zs;
        zskiplistNode *node;
      } sl;
    } zset;
  } iter;
} zsetopsrc;

void zuiInitIterator(zsetopsrc *op);
int zuiNext(zsetopsrc *op, zsetopval *val);
robj *zuiObjectFromValue(zsetopval *val);
int zuiFind(zsetopsrc *op, zsetopval *val, double *score);
void zuiClearIterator(zsetopsrc *op);

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
  zsetopval zval;
  zsetopsrc *zset;
  robj *cap, *o, *tmp, **filters, *hash;
  robj *summary_field = createStringObject("summary", 7);

  if ((getLongFromObjectOrReply(c, c->argv[3], &filter_count, NULL) != REDIS_OK)) { goto end; }
  zset = zcalloc(sizeof(zsetopsrc));
  filters = zmalloc(sizeof(robj*) * filter_count);

  if ((zset->subject = lookupKeyReadOrReply(c, c->argv[1], shared.emptymultibulk)) == NULL || checkType(c, zset->subject, REDIS_ZSET)) { goto end; }
  zset->encoding = zset->subject->encoding;
  zset->type = REDIS_ZSET;

  if ((cap = lookupKeyReadOrReply(c, c->argv[2], shared.emptymultibulk)) == NULL || checkType(c, cap, REDIS_SET)) { goto end; }

  for(int i = 0; i < filter_count; i++) {
    if ((filters[i] = lookupKeyReadOrReply(c, c->argv[i+4], shared.emptymultibulk)) == NULL || checkType(c, cap, REDIS_SET)) { goto end; }
  }

  if (!strcasecmp(c->argv[4 + filter_count]->ptr, "asc")) { desc = 0; }
  if((getLongFromObjectOrReply(c, c->argv[5 + filter_count], &offset, NULL) != REDIS_OK)) { return; }
  if((getLongFromObjectOrReply(c, c->argv[6 + filter_count], &count, NULL) != REDIS_OK)) { return; }

  zuiInitIterator(zset);
  memset(&zval, 0, sizeof(zval));

  replylen = addDeferredMultiBulkLength(c);

  while (zuiNext(zset, &zval)) {
    tmp = zuiObjectFromValue(&zval);
    skip = 0;
    for(int i = 0; i < filter_count; ++i) {
      if (!setTypeIsMember(filters[i], tmp)) {
        skip = 1;
        break;
      }
    }
    if (skip == 1 || setTypeIsMember(cap, tmp)) { continue; }

    if (found++ >= offset && added < count) {
      field_length = strlen(tmp->ptr);
      hash = createStringObject(NULL, field_length + 2);
      k = hash->ptr;
      memcpy(k, "r:", 2);
      memcpy(k + 2, tmp->ptr, field_length);
      o = lookupKeyRead(c->db, hash);
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
    if (found > 500 && added == count) { break; }
  }

  addReplyLongLong(c, found);
  zuiClearIterator(zset);
end:
  zfree(filters);
  zfree(zset);
  decrRefCount(summary_field);
  setDeferredMultiBulkLength(c, replylen, added + 1);
}
