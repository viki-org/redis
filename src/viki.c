#include "server.h"
#include "viki.h"

static robj *getResourceValue(sds ele, int blocked);

int replyWithDetail(client *c, sds item, int blocked) {
    robj *value = getResourceValue(item, blocked);
    if (value == NULL) {
        return 0;
    }

    addReplyBulk(c, value);
    decrRefCount(value);

    return 1;
}

static robj *getResourceValue(sds item, int blocked) {
    int id_length = sdslen(item);

    int block_length = blocked == 1 ? 17 : 18; // '","blocked":true}'  or '","blocked":false}'
    robj *details = createStringObject(NULL, 7 + id_length + block_length);

    char *p = details->ptr;
    memcpy(p, "{\"id\":\"", 7);
    p += 7;

    memcpy(p, item, id_length);
    p += id_length;

    if (blocked == 1) {
        memcpy(p, "\",\"blocked\":true}", 17);
    } else {
        memcpy(p, "\",\"blocked\":false}", 18);
    }

    return details;
}

robj **loadSetArrayIgnoreMiss(client *c, int offset, long *count) {
    long total = *count;
    if (total == 0) { return NULL; }

    long misses = 0;
    robj **array = zmalloc(sizeof(robj * ) * total);
    for (int i = 0; i < total; ++i) {
        robj *set;

        if (((set = lookupKeyRead(c->db, c->argv[i + offset])) == NULL) || set->type != OBJ_SET) {
            ++misses;
        } else {
            array[i - misses] = set;
        }
    }
    *count = total - misses;

    return array;
}

robj **loadSetArray(client *c, int offset, long count) {
    if (count == 0) { return NULL; }

    robj **array = zmalloc(sizeof(robj * ) * count);
    for (int i = 0; i < count; ++i) {
        robj *set;

        if (((set = lookupKeyRead(c->db, c->argv[i + offset])) == NULL) || set->type != OBJ_SET) {
            goto miss;
        } else {
            array[i] = set;
        }
    }

    return array;

miss:
    zfree(array);
    return NULL;
}

int isMemberOfAnySet(robj **sets, long sets_count, sds ele) {
    for (int i = 0; i < sets_count; ++i) {
        if (setTypeIsMember(sets[i], ele)) {
            return 1;
        }
    }

    return 0;
}

int isMemberOfAllSets(robj **sets, long sets_count, sds ele) {
    for (int i = 0; i < sets_count; ++i) {
        if (!setTypeIsMember(sets[i], ele)) {
            return 0;
        }
    }

    return 1;
}

int isBlocked(long allow_count, robj **allows, long block_count, robj **blocks, sds ele) {
    if (block_count == 0) {
	return 0;
    }

    if (isMemberOfAnySet(allows, allow_count, ele)) {
        return 0;
    }

    if (isMemberOfAnySet(blocks, block_count, ele)) {
        return 1;
    }

    return 0;
}
