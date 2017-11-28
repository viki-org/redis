#ifndef VIKI_H
#define VIKI_H

int qsortCompareSetsByCardinality(const void *s1, const void *s2);

int replyWithDetail(client *c, sds ele, int blocked);

robj **loadSetArrayIgnoreMiss(client *c, int offset, long *count);

robj **loadSetArray(client *c, int offset, long count);

int isBlocked(long allow_count, robj **allows, long block_count, robj **blocks, sds ele);

int isMemberOfAnySet(robj **sets, long sets_count, sds ele);

int isMemberOfAllSets(robj **sets, long sets_count, sds ele);

int isSetsIntersect(robj *sa, robj *sb);

#endif
