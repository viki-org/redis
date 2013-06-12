int qsortCompareSetsByCardinality(const void *s1, const void *s2);
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score);
int replyWithDetail(redisClient *c, robj *item, robj *detail_field);
double getScore(robj *zset, robj *item);
robj *getResourceValue(redisClient *c, robj *item, robj *field);
robj *getHashValue(redisClient *c, robj *item, robj *field);
robj *generateKey(robj *item);
robj *mergeBrickResourceDetails(redisClient *c, robj *brick, robj *key, robj *field);
inline int isMember(dict *subject, robj *item);

int checkTypes(redisClient *c, robj *o, int *types);
int checkTypeNoReply(robj *o, int type);
int belongTo(robj *collection, robj *item);

inline int heldback(long incl_count, robj **inclusiveLists, long excl_count, robj **exclusiveLists, robj *item) {
  for (long i = 0; i < incl_count; ++i) {
    if (inclusiveLists[i] != NULL && belongTo(inclusiveLists[i], item)) { return 0; }
  }
  for (long i = 0; i < excl_count; ++i) {
    if (exclusiveLists[i] != NULL && belongTo(exclusiveLists[i], item)) { return 1; }
  }
  return 0;
}

inline int isMember(dict *subject, robj *item) {
  return dictFind(subject, item) != NULL;
}
