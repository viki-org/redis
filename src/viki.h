typedef struct vikiResultMetadata {
  int blocked;
} vikiResultMetadata;

int qsortCompareSetsByCardinality(const void *s1, const void *s2);
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score);
int replyWithDetail(redisClient *c, robj *item, robj *detail_field);
int replyWithMetadata(redisClient *c, robj *metadataObj);
double getScore(robj *zset, robj *item);
robj *getResourceValue(redisClient *c, robj *item, robj *field);
robj *getHashValue(redisClient *c, robj *item, robj *field);
robj *generateKey(robj *item);
robj *mergeBrickResourceDetails(redisClient *c, robj *brick, robj *key, robj *field);

inline int isMember(dict *subject, robj *item) {
  return dictFind(subject, item) != NULL;
}

inline int heldback(dict *cap, dict *anti_cap, robj *inclusiveList, dict *exclusiveList, robj *item) {
  if (inclusiveList != NULL && getScore(inclusiveList, item) != -1) { return 0; }
  if (exclusiveList != NULL && isMember(exclusiveList, item)) { return 1; }
  if (cap == NULL || !isMember(cap, item)) { return 0; }
  return (anti_cap == NULL || !isMember(anti_cap, item));
}

robj *generateMetadataObject(vikiResultMetadata *metadata);
