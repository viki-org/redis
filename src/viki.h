int qsortCompareSetsByCardinality(const void *s1, const void *s2);
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score);
int replyWithDetail(redisClient *c, robj *item);
double getScore(robj *zset, robj *item);
robj *getResourceValue(redisClient *c, robj *item);
robj *getHashValue(redisClient *c, robj *item, robj *field);
robj *generateKey(robj *item);
robj *mergeBrickResourceDetails(redisClient *c, robj *brick, robj *key, robj *field);
dict **loadSetArray(redisClient *c, int offset, long *count);

inline int isMember(dict *subject, robj *item) {
  return dictFind(subject, item) != NULL;
}

inline int heldback(long allow_count, dict **allows, long block_count, dict **blocks, robj *item) {
  for (int i = 0; i < allow_count; ++i) {
    if (isMember(allows[i], item)) { return 0; }
  }
  for (int i = 0; i < block_count; ++i) {
    if (isMember(blocks[i], item)) { return 1; }
  }
  return 0;
}
