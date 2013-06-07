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

inline int heldback(dict *cap, dict *anti_cap, robj *inclusiveList, robj *exclusiveList, robj *item) {
  if (inclusiveList != NULL && belongTo(inclusiveList, item)) { return 0; }
  if (exclusiveList != NULL && belongTo(exclusiveList, item)) { return 1; }
  if (cap == NULL || !isMember(cap, item)) { return 0; }
  return (anti_cap == NULL || !isMember(anti_cap, item));
}

inline int isMember(dict *subject, robj *item) {
  return dictFind(subject, item) != NULL;
}
