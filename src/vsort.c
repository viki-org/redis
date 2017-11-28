#include "server.h"
#include "viki.h"

typedef struct vsortData {
    int added, id_offset;
    long allow_count, block_count, id_count, count;
    robj **allows;
    robj **blocks;
    robj *zscores;
} vsortData;

typedef struct {
    sds item;
    double score;
} itemScore;

static void vsortByViews(client *c, vsortData *data);

static void vsortByNone(client *c, vsortData *data);

static int itemScoreComparitor(const void *lhs, const void *rhs);

// count zset allow_count [allows] block_count [blocks] resource_count [resources]
void vsortCommand(client *c) {
    int block_offset;
    long allow_count, block_count, count;
    void *replylen;
    robj *zscores;
    vsortData *data;

    if ((getLongFromObjectOrReply(c, c->argv[1], &count, NULL) != C_OK)) { return; }
    if ((zscores = lookupKeyRead(c->db, c->argv[2])) != NULL && checkType(c, zscores, OBJ_ZSET)) { return; }

    if ((getLongFromObjectOrReply(c, c->argv[3], &allow_count, NULL) != C_OK)) { return; }
    block_offset = 4 + allow_count;
    if ((getLongFromObjectOrReply(c, c->argv[block_offset], &block_count, NULL) != C_OK)) { return; }

    data = zmalloc(sizeof(*data));
    data->added = 0;
    data->count = count;
    data->zscores = zscores;
    data->id_offset = 6 + allow_count + block_count;

    data->allows = loadSetArrayIgnoreMiss(c, 4, &allow_count);
    data->allow_count = allow_count;

    data->blocks = loadSetArrayIgnoreMiss(c, block_offset + 1, &block_count);
    data->block_count = block_count;

    replylen = addDeferredMultiBulkLength(c);
    if (count < c->argc - data->id_offset) {
        vsortByViews(c, data);
    } else {
        vsortByNone(c, data);
    }
    setDeferredMultiBulkLength(c, replylen, data->added);
    if (data->allows != NULL) { zfree(data->allows); }
    if (data->blocks != NULL) { zfree(data->blocks); }
    zfree(data);
}

static void vsortByViews(client *c, vsortData *data) {
    long allow_count = data->allow_count;
    long block_count = data->block_count;
    long count = data->count;
    int found = 0, lowest_at = 0;
    double lowest = -1;
    double scores[count];

    robj *zscores = data->zscores;
    robj **allows = data->allows;
    robj **blocks = data->blocks;
    sds *items = zmalloc(sizeof(sds) * count);

    for (int i = data->id_offset; i < c->argc; ++i) {
        sds member = c->argv[i]->ptr;
        double score;

        if (isBlocked(allow_count, allows, block_count, blocks, member)) {
            continue;
        }
        zsetScore(zscores, member, &score);
        if (found < count) {
            items[found] = member;
            if (lowest == -1 || score <= lowest) {
                //it's better to replace later items with the same score, as the input might be sorted some way (it is!)
                lowest = score;
                lowest_at = found;
            }
            scores[found++] = score;
        } else if (score > lowest) {
            items[lowest_at] = member;
            scores[lowest_at] = score;
            lowest = scores[0];
            lowest_at = 0;
            for (int j = 1; j < count; ++j) {
                if (scores[j] <= lowest) {
                    lowest = scores[j];
                    lowest_at = j;
                }
            }
        }
    }

    if (found > 1) {
        itemScore map[found];
        for (int i = 0; i < found; ++i) {
            map[i].item = items[i];
            map[i].score = scores[i];
        }
        qsort(map, found, sizeof(itemScore), itemScoreComparitor);
        for (int i = 0; i < found; ++i) {
            if (replyWithDetail(c, map[i].item, 0)) {
                ++(data->added);
            }
        }
    } else if (found == 1) {
        if (replyWithDetail(c, items[0], 0)) {
            ++(data->added);
        }
    }
    zfree(items);
}

static void vsortByNone(client *c, vsortData *data) {
    long allow_count = data->allow_count;
    long block_count = data->block_count;
    robj **allows = data->allows;
    robj **blocks = data->blocks;

    for (int i = data->id_offset; i < c->argc; ++i) {
        sds ele = c->argv[i]->ptr;
        if (isBlocked(allow_count, allows, block_count, blocks, ele)) { continue; }
        if (replyWithDetail(c, ele, 0)) {
            ++(data->added);
        }
    }
}

static int itemScoreComparitor(const void *lhs, const void *rhs) {
    return (*(itemScore *) rhs).score - (*(itemScore *) lhs).score;
}
