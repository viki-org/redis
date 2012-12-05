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

void vdiffstoreCommand(redisClient *c) {
    int touched = 0;
    int found = 0;
    int added = 0;
    double value;
    long offset, count;
    zsetopval zval;
    zsetopsrc *src;
    robj *tmp;

    robj *dstkey = c->argv[1];
    robj *dstobj = createZsetObject();
    zset *dstzset = dstobj->ptr;

    src = zcalloc(sizeof(zsetopsrc) * 2);
    src[0].subject = lookupKey(c->db,c->argv[2]);
    if (src[0].subject == NULL) {
        goto vikidiffstorageend;
    } else {
        src[0].encoding = src[0].subject->encoding;
    }
    src[0].type = REDIS_ZSET;
    zuiInitIterator(&src[0]);

    src[1].subject = lookupKey(c->db,c->argv[3]);
    if (src[1].subject != NULL) {
        src[1].encoding = src[1].subject->encoding;
    }
    src[1].type = REDIS_SET;
    zuiInitIterator(&src[1]);

    getLongFromObjectOrReply(c, c->argv[4], &offset, NULL);
    getLongFromObjectOrReply(c, c->argv[5], &count, NULL);

    memset(&zval, 0, sizeof(zval));

    while (zuiNext(&src[0],&zval)) {
        tmp = zuiObjectFromValue(&zval);
        if (zuiFind(&src[1],&zval,&value) == 0) {
            if (found++ >= offset && added < count) {
                zslInsert(dstzset->zsl,zval.score,tmp);
                incrRefCount(tmp); /* added to skiplist */
                dictAdd(dstzset->dict,tmp,&zval.score);
                incrRefCount(tmp); /* added to dictionary */
                ++added;
            }
        }
    }

    zuiClearIterator(&src[0]);
    zuiClearIterator(&src[1]);

    if (dbDelete(c->db,dstkey)) {
        signalModifiedKey(c->db,dstkey);
        touched = 1;
        server.dirty++;
    }

vikidiffstorageend:
    if (dstzset->zsl->length) {
        dbAdd(c->db,dstkey,dstobj);
        addReplyLongLong(c,found);
        if (!touched) signalModifiedKey(c->db,dstkey);
        server.dirty++;
    } else {
        decrRefCount(dstobj);
        addReply(c,shared.czero);
    }
    zfree(src);
}
