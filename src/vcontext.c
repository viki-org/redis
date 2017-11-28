#include "server.h"
#include "viki.h"

typedef struct vcontextData {
    long index_offset;
    long allow_count, block_count, index_count, filter_count;
    robj **indices;
    robj **allows;
    robj **blocks;
    robj **filters;
    int added;
} vcontextData;

static void vcontextWithFilters(client *c, vcontextData *data);

static void vcontextWithOneFilter(client *c, vcontextData *data);

static void vcontextWithoutFilters(client *c, vcontextData *data);

// vcontext allow_count [allows] block_count [blocks] filter_count [filters] index_count [indexes]
void vcontextCommand(client *c) {
    long allow_count, block_count, block_offset, filter_count, filter_offset, index_count, index_offset;
    vcontextData *data;
    void *replylen;

    if ((getLongFromObjectOrReply(c, c->argv[1], &allow_count, NULL) != C_OK)) { return; }
    block_offset = 2 + allow_count;

    if ((getLongFromObjectOrReply(c, c->argv[block_offset], &block_count, NULL) != C_OK)) { return; }
    filter_offset = 3 + allow_count + block_count;

    if ((getLongFromObjectOrReply(c, c->argv[filter_offset], &filter_count, NULL) != C_OK)) { return; }
    index_offset = 4 + allow_count + block_count + filter_count;

    if ((getLongFromObjectOrReply(c, c->argv[index_offset], &index_count, NULL) != C_OK)) { return; }

    data = zmalloc(sizeof(*data));
    data->indices = zmalloc(sizeof(robj * ) * index_count);
    data->added = 0;
    data->index_offset = index_offset;

    replylen = addDeferredMultiBulkLength(c);

    data->allows = loadSetArrayIgnoreMiss(c, 2, &allow_count);
    data->allow_count = allow_count;

    data->blocks = loadSetArrayIgnoreMiss(c, block_offset + 1, &block_count);
    data->block_count = block_count;

    data->filters = loadSetArray(c, filter_offset + 1, filter_count);
    if (data->filters == NULL && filter_count > 0) { goto reply; }
    data->filter_count = filter_count;

    data->index_count = index_count;
    for (int i = 0; i < index_count; ++i) {
        if ((data->indices[i] = lookupKeyRead(c->db, c->argv[i + index_offset + 1])) != NULL &&
            checkType(c, data->indices[i], OBJ_SET)) { goto reply; }
    }

    if (data->filter_count == 0) {
        vcontextWithoutFilters(c, data);
    } else if (data->filter_count == 1) {
        vcontextWithOneFilter(c, data);
    } else {
        vcontextWithFilters(c, data);
    }

reply:
    setDeferredMultiBulkLength(c, replylen, data->added);

    if (data->allows != NULL) { zfree(data->allows); }
    if (data->blocks != NULL) { zfree(data->blocks); }
    if (data->filters != NULL) { zfree(data->filters); }

    zfree(data->indices);
    zfree(data);
}

static void vcontextWithOneFilter(client *c, vcontextData *data) {
    robj *filter = data->filters[0];
    size_t filter_size = setTypeSize(filter);
    for(int i = 0; i < data->index_count; ++i) {
        if (data->indices[i] == NULL) {
            continue;
        }

        robj *sa = filter;
        robj *sb = data->indices[i];
        size_t index_size = setTypeSize(sb);
        if (filter_size > index_size) {
            robj *tmp = sa;
            sa = sb;
            sb = tmp;
        }

        setTypeIterator *si = setTypeInitIterator(sa);
        sds ele;
        int64_t intele;
        while ((setTypeNext(si, &ele, &intele)) != -1) {
            if (!setTypeIsMember(sb, ele)) {
                continue;
            }

            if (isBlocked(data->allow_count, data->allows, data->block_count, data->blocks, ele)) {
                continue;
            }

            addReplyBulk(c, c->argv[i + data->index_offset + 1]);
            data->added++;
            break;
        }

        setTypeReleaseIterator(si);
    }
}

static void vcontextWithFilters(client *c, vcontextData *data) {
    /**
     * filter ^ filter[1:n] ^ allows - blocks
     */
    qsort(data->filters, data->filter_count, sizeof(robj * ), qsortCompareSetsByCardinality);

    setTypeIterator *si = setTypeInitIterator(data->filters[0]);
    int64_t intele;
    sds ele;
    robj *dstobj = createSetObject();
    while ((setTypeNext(si, &ele, &intele)) != -1) {
        if(data->filter_count > 1 && !isMemberOfAllSets(&data->filters[1], data->filter_count - 1, ele))  {
            continue;
        }

        if (isBlocked(data->allow_count, data->allows, data->block_count, data->blocks, ele)) {
            continue;
        }

        setTypeAdd(dstobj, ele);
    }
    setTypeReleaseIterator(si);

    /**
     * index[0] ^ set + index[1] ^ set + ...
     */
    for (int i = 0; i < data->index_count; ++i) {
        if (data->indices[i] == NULL) {
            continue;
        }

        if(isSetsIntersect(dstobj, data->indices[i])) {
            addReplyBulk(c, c->argv[i + data->index_offset + 1]);
            data->added++;
        }
    }

    decrRefCount(dstobj);
}

static void vcontextWithoutFilters(client *c, vcontextData *data) {
    for (int i = 0; i < data->index_count; ++i) {
        if (data->indices[i] == NULL) { continue; }

        setTypeIterator * si = setTypeInitIterator(data->indices[i]);
        int64_t intele;
        sds ele;
        while ((setTypeNext(si, &ele, &intele)) != -1) {
            if (isBlocked(data->allow_count, data->allows, data->block_count, data->blocks, ele)) {
                continue;
            }

            addReplyBulk(c, c->argv[i + data->index_offset + 1]);
            data->added++;
            break;
        }
        setTypeReleaseIterator(si);
    }
}
