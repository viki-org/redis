#include "server.h"
#include "viki.h"

typedef struct vfindData {
    int desc, found, added, include_blocked;
    long allow_count, block_count, filter_count, offset, count, up_to;
    robj **allows, **blocks, **filters;
    zskiplistNode *ln;
    zset *zset;
} vfindData;

zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank);

static void vfindByZSet(client *c, vfindData *vfind);

static void vfindBySmallestFilter(client *c, vfindData *vfind);

static void initializeZsetIterator(vfindData *data);

int *vfindGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys) {
    UNUSED(cmd);

    if (argc < 10) { return NULL; }
    int allow_count = atoi(argv[7]->ptr);

    int block_offset = 8 + allow_count;
    if (argc < (10 + allow_count)) {return NULL; }
    int block_count = atoi(argv[block_offset]->ptr);

    int filter_offset = 9 + allow_count + block_count;
    if (argc < (10 + allow_count + block_count)) {return NULL; }
    int filter_count = atoi(argv[filter_offset]->ptr);

    int num = 1 + block_count + allow_count + filter_count;  //1 is for the zset
    int * keys = zmalloc(sizeof(int) * num);
    // zset key position
    keys[0] = 1;

    int offset = 1;

    for (int i = 0; i < allow_count; ++i) {
        keys[offset++] = 8 + i;
    }
    for (int i = 0; i < block_count; ++i) {
        keys[offset++] = block_offset + i;
    }
    for (int i = 0; i < filter_count; ++i) {
        keys[offset++] = filter_offset + i;
    }

    *numkeys = num;
    return keys;
}

// Prepares all the sets and calls vfindBySmallestFilter or vfindByZSet
// depending on the ratio of the input set and the smallest filter set
// Syntax:
// vfind zset offset count upto direction include_blocked allow_count [allows] block_count [blocks] filter_count [filter keys]
void vfindCommand(client *c) {
    long allow_count, block_count, block_offset, filter_count, filter_offset;
    void *replylen;
    long offset, count, up_to;
    robj *items, *direction, *include_blocked;
    vfindData *data;

    // All the data checks
    if ((items = lookupKeyRead(c->db, c->argv[1])) == NULL) {
        addReplyMultiBulkLen(c, 1);
        addReplyLongLong(c, 0);
        return;
    }
    if (checkType(c, items, OBJ_ZSET)) { return; }
    if ((getLongFromObjectOrReply(c, c->argv[2], &offset, NULL) != C_OK)) { return; }
    if ((getLongFromObjectOrReply(c, c->argv[3], &count, NULL) != C_OK)) { return; }
    if ((getLongFromObjectOrReply(c, c->argv[4], &up_to, NULL) != C_OK)) { return; }
    direction = c->argv[5];
    include_blocked = c->argv[6];

    if ((getLongFromObjectOrReply(c, c->argv[7], &allow_count, NULL) != C_OK)) { return; }
    block_offset = 8 + allow_count;
    if ((getLongFromObjectOrReply(c, c->argv[block_offset], &block_count, NULL) != C_OK)) { return; }
    filter_offset = 9 + allow_count + block_count;
    if ((getLongFromObjectOrReply(c, c->argv[filter_offset], &filter_count, NULL) != C_OK)) { return; }

    data = zmalloc(sizeof(*data));
    data->filters = NULL;
    data->offset = offset;
    data->count = count;
    data->up_to = up_to;
    data->added = 0;
    data->found = 0;
    data->desc = 1;
    data->include_blocked = 0;

    replylen = addDeferredMultiBulkLength(c);

    zsetConvert(items, OBJ_ENCODING_SKIPLIST);
    data->zset = items->ptr;

    if (!strcasecmp(direction->ptr, "asc")) { data->desc = 0; }
    // The keyword for blocked items is "withblocked"
    if (!strcasecmp(include_blocked->ptr, "withblocked")) { data->include_blocked = 1; }

    data->allows = loadSetArrayIgnoreMiss(c, 8, &allow_count);
    data->allow_count = allow_count;

    data->blocks = loadSetArrayIgnoreMiss(c, block_offset + 1, &block_count);
    data->block_count = block_count;

    data->filter_count = filter_count;
    data->filters = NULL;
    if (filter_count != 0) {
        data->filters = loadSetArrayIgnoreMiss(c, filter_offset + 1, &filter_count);
        data->filter_count = filter_count;
        if(data->filter_count == 0) {
            // non existing filters
            goto reply;
        }

        qsort(data->filters, filter_count, sizeof(robj * ), qsortCompareSetsByCardinality);
        // Size of smallest filter
        int size = setTypeSize(data->filters[0]);
        int ratio = zsetLength(items) / size;

        if ((size < 100 && ratio > 1) || (size < 500 && ratio > 2) || (size < 2000 && ratio > 3)) {
            vfindBySmallestFilter(c, data);
            goto reply;
        }
    }

    initializeZsetIterator(data);
    vfindByZSet(c, data);

reply:
    addReplyLongLong(c, data->found);
    setDeferredMultiBulkLength(c, replylen, (data->added) + 1);

    if (data->allows != NULL) { zfree(data->allows); }
    if (data->blocks != NULL) { zfree(data->blocks); }
    if (data->filters != NULL) { zfree(data->filters); }
    zfree(data);
}

/**
 * Add element to zset without check its existence to get better performance.
 *
 * @param s
 * @param ele
 * @param score
 */
static void vfindZSetAdd(zset *s, sds ele, double score) {
    sds item = sdsdup(ele);
    zskiplistNode * znode = zslInsert(s->zsl, score, item);
    serverAssert(dictAdd(s->dict, item, &znode->score) == DICT_OK);
}

// Used if the smallest filter has a small size compared to zset's size
void vfindBySmallestFilter(client *c, vfindData *vfind) {
    zset *dstzset;

    int found = 0;
    int added = 0;

    robj *dstobj = createZsetObject();
    dstzset = dstobj->ptr;

    robj *objSetBlocked = NULL;
    if(vfind->include_blocked) {
        objSetBlocked = createSetObject();
    }

    setTypeIterator *si = setTypeInitIterator(vfind->filters[0]);
    int64_t intobj;
    sds item;
    zset *zset = vfind->zset;
    while ((setTypeNext(si, &item, &intobj)) != -1) {
        dictEntry *de = dictFind(zset->dict, item);
        if (de == NULL) {
            continue;
        }

        if(vfind->filter_count > 1 && !isMemberOfAllSets(&vfind->filters[1], vfind->filter_count - 1, item))  {
            continue;
        }

        int blocked = isBlocked(vfind->allow_count, vfind->allows, vfind->block_count, vfind->blocks, item);
        if (blocked && !vfind->include_blocked) {
            continue;
        }

        double score = *(double *) dictGetVal(de);
        vfindZSetAdd(dstzset, item, score);

        if (blocked) {
            setTypeAdd(objSetBlocked, item);
        }
        ++found;
    }

    setTypeReleaseIterator(si);

    if (found > vfind->offset) {
        zskiplistNode *ln;

        if (vfind->desc) {
            ln = vfind->offset == 0 ? dstzset->zsl->tail : zslGetElementByRank(dstzset->zsl, found - vfind->offset);
        } else {
            ln = vfind->offset == 0 ? dstzset->zsl->header->level[0].forward : zslGetElementByRank(dstzset->zsl, vfind->offset + 1);
        }

        while (added < found && added < vfind->count && ln != NULL) {
            int blocked = vfind->include_blocked ? setTypeIsMember(objSetBlocked, ln->ele) : 0;
            if (replyWithDetail(c, ln->ele, blocked)) {
                added++;
            } else {
                --found;
            }

            ln = vfind->desc? ln->backward : ln->level[0].forward;
        }
    }

    decrRefCount(dstobj);

    if(vfind->include_blocked) {
        decrRefCount(objSetBlocked);
    }
    vfind->found = found;
    vfind->added = added;
}

// Used if the smallest filter has a relatively big size compared to zset's size
void vfindByZSet(client *c, vfindData *vfind) {
    int found = 0;
    int added = 0;

    for (zskiplistNode *ln = vfind->ln; ln != NULL; ln = vfind->desc ? ln->backward : ln->level[0].forward) {
        sds item = ln->ele;

        if (vfind->filter_count > 0 && !isMemberOfAllSets(vfind->filters, vfind->filter_count, item)) {
            continue;
        }

        int blocked = isBlocked(vfind->allow_count, vfind->allows, vfind->block_count, vfind->blocks, item);
        if (blocked && !vfind->include_blocked) {
            continue;
        }

        if (found++ >= vfind->offset && added < vfind->count) {
            if (replyWithDetail(c, item, blocked)) {
                added++;
            } else {
                --found;
            }
        }

        if (added == vfind->count && found >= vfind->up_to) {
            break;
        }
    }

    vfind->found = found;
    vfind->added = added;
}

static void initializeZsetIterator(vfindData *data) {
    zskiplist *zsl;
    zsl = data->zset->zsl;
    data->ln = data->desc ? zsl->tail : zsl->header->level[0].forward;
}
