#include "server.h"
#include "viki.h"

typedef struct vcontextData {
    int added;
    long allow_count, block_count, index_count, filter_count, index_offset;
    robj **indices;
    robj **allows;
    robj **blocks;
    robj **filters;
} vcontextData;

typedef struct indexNode {
    int i;
    robj *index;
    struct indexNode *next;
} indexNode;

void vcontextWithFilters(client *c, vcontextData *data);

void vcontextWithOneFilter(client *c, vcontextData *data);

void vcontextWithoutFilters(client *c, vcontextData *data);

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

void vcontextWithOneFilter(client *c, vcontextData *data) {
    sds ele;
    long allow_count = data->allow_count;
    long block_count = data->block_count;
    long index_offset = data->index_offset;
    robj **allows = data->allows;
    robj **blocks = data->blocks;
    robj **indexes = data->indices;

    robj *filter = data->filters[0];

    setTypeIterator *si = setTypeInitIterator(filter);

    indexNode *head = zmalloc(sizeof(indexNode));
    head->i = -1;
    indexNode *last = head;
    for (int i = 0; i < data->index_count; ++i) {
        if (indexes[i] == NULL) { continue; }
        indexNode *node = zmalloc(sizeof(indexNode));
        node->i = i;
        node->index = indexes[i];
        last->next = node;
        last = node;
    }
    last->next = NULL;

    int64_t intele;
    while ((setTypeNext(si, &ele, &intele)) != -1) {
        if (isBlocked(allow_count, allows, block_count, blocks, ele)) {
            continue;
        }

        indexNode *last = head;
        for (indexNode *n = head->next; n != NULL; n = n->next) {
            if (setTypeIsMember(n->index, ele)) {
                ++(data->added);
                addReplyBulk(c, c->argv[n->i + index_offset + 1]);
                last->next = n->next;
                zfree(n);
                if (head->next == NULL) { goto cleanup; }
            } else {
                last = n;
            }
        }
    }

cleanup:

    {
        indexNode *n = head;
        while (1) {
            if (n == NULL) { break; }
            indexNode *curr = n;
            n = curr->next;
            zfree(curr);
        }
    }

    setTypeReleaseIterator(si);
}

void vcontextWithFilters(client *c, vcontextData *data) {
    sds ele;
    long allow_count = data->allow_count;
    long block_count = data->block_count;
    robj **allows = data->allows;
    robj **blocks = data->blocks;

    qsort(data->filters, data->filter_count, sizeof(robj * ), qsortCompareSetsByCardinality);

    robj **filters = data->filters;
    robj *dstobj = createSetObject();

    /**
     * filter ^ filter[1:n] ^ allows - blocks
     */
    setTypeIterator *si = setTypeInitIterator(filters[0]);
    int64_t intele;
    while ((setTypeNext(si, &ele, &intele)) != -1) {
        if(data->filter_count > 1 && !isMemberOfAllSets(&data->filters[1], data->filter_count - 1, ele))  {
            continue;
        }

        if (isBlocked(allow_count, allows, block_count, blocks, ele)) {
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
            ++(data->added);
            addReplyBulk(c, c->argv[i + data->index_offset + 1]);
        }
    }

    decrRefCount(dstobj);
}

void vcontextWithoutFilters(client *c, vcontextData *data) {
    for (int i = 0; i < data->index_count; ++i) {
        if (data->indices[i] == NULL) { continue; }

        setTypeIterator * si = setTypeInitIterator(data->indices[i]);
        int64_t intele;
        sds ele;
        while ((setTypeNext(si, &ele, &intele)) != -1) {
            if (isBlocked(data->allow_count, data->allows, data->block_count, data->blocks, ele)) {
                continue;
            }

            ++(data->added);
            addReplyBulk(c, c->argv[i + data->index_offset + 1]);
            break;
        }
        setTypeReleaseIterator(si);
    }
}
