#include "server.h"
#include "viki.h"

// export internal method to speed up output.
extern void addReplyLongLongWithPrefix(client *c, long long ll, char prefix);

int replyWithDetail(client *c, sds item, int blocked) {
    size_t id_length = sdslen(item);
    size_t len = 7 + id_length + (blocked? 17 : 18);

    addReplyLongLongWithPrefix(c, len, '$');

    addReplyString(c,"{\"id\":\"", 7);
    addReplyString(c, item, id_length);
    if(blocked) {
        addReplyString(c, "\",\"blocked\":true}", 17);
    } else {
        addReplyString(c, "\",\"blocked\":false}", 18);
    }

    addReply(c,shared.crlf);

    return 1;
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

/**
 * whether two sets has at least one common member or not
 *
 * @param sa
 * @param sb
 * @return
 */
int isSetsIntersect(robj *sa, robj *sb) {
    if (sa == NULL || sb == NULL) return 0;

    size_t la = setTypeSize(sa);
    size_t lb = setTypeSize(sb);

    if (la == 0 || lb == 0) return 0;
    if (la > lb) {
        robj *tmp = sa;
        sa = sb;
        sb = tmp;
    }

    setTypeIterator *si = setTypeInitIterator(sa);
    sds ele;
    int64_t intele;
    while ((setTypeNext(si, &ele, &intele)) != -1) {
        if (setTypeIsMember(sb, ele)) {
            setTypeReleaseIterator(si);
            return 1;
        }
    }

    setTypeReleaseIterator(si);

    return 0;
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
