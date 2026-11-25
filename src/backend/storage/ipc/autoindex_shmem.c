/*-------------------------------------------------------------------------
 *
 * autoindex_shmem.c
 *	  Shared memory hash table for tracking sequential scans.
 *
 * Candidates are keyed on (database, relation, sorted column set).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <stddef.h>

#include "access/transam.h"
#include "miscadmin.h"
#include "storage/autoindex_shmem.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "fmgr.h"
#include "utils/hsearch.h"

static HTAB *AutoIndexHash = NULL;
static AutoIndexSharedState *AutoIndexState = NULL;

/* Check that a column set is usable as a key. */
static bool
AutoIndexKeyValid(Oid relid, const AttrNumber *attnums, int ncols)
{
	int i;

	if (!OidIsValid(relid) || attnums == NULL)
		return false;

	if (ncols < 1 || ncols > AUTO_INDEX_MAX_COLUMNS)
		return false;

	for (i = 0; i < ncols; i++)
		if (attnums[i] <= 0)
			return false;

	return true;
}

/* Build a hash key with sorted attnums and zeroed unused slots. */
static void
AutoIndexMakeKey(Oid relid, const AttrNumber *attnums, int ncols, AutoIndexKey *key)
{
	AttrNumber sorted[AUTO_INDEX_MAX_COLUMNS];
	int i, j;

	memset(sorted, 0, sizeof(sorted));
	memcpy(sorted, attnums, ncols * sizeof(AttrNumber));

	for (i = 1; i < ncols; i++)
	{
		AttrNumber v = sorted[i];

		for (j = i - 1; j >= 0 && sorted[j] > v; j--)
			sorted[j + 1] = sorted[j];
		sorted[j + 1] = v;
	}

	memset(key, 0, sizeof(*key));
	key->datid = MyDatabaseId;
	key->relid = relid;
	key->ncols = ncols;
	memcpy(key->attnums, sorted, sizeof(key->attnums));
}

Size
AutoIndexShmemSize(void)
{
	Size size;

	size = sizeof(AutoIndexSharedState);
	size = add_size(size, hash_estimate_size(AUTO_INDEX_MAX_CANDIDATES, sizeof(AutoIndexEntry)));
	return size;
}

void
AutoIndexShmemInit(void)
{
	bool found;
	HASHCTL info;

	AutoIndexState = (AutoIndexSharedState *) ShmemInitStruct("AutoIndex Shared State", sizeof(AutoIndexSharedState), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		LWLockInitialize(&AutoIndexState->lock, LWTRANCHE_FIRST_USER_DEFINED);
	}
	else
	{
		Assert(found);
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(AutoIndexKey);
	info.entrysize = sizeof(AutoIndexEntry);
	info.hash = tag_hash;

	AutoIndexHash = ShmemInitHash("AutoIndex Candidate Hash", AUTO_INDEX_MAX_CANDIDATES, AUTO_INDEX_MAX_CANDIDATES, &info, HASH_ELEM | HASH_BLOBS);
}

/* Format a column list for logging, e.g. "3[1,2,4]". */
static void
AutoIndexFormatColumns(const AutoIndexKey *key, char *buf, Size bufsize)
{
	int i;
	Size len = 0;

	len = snprintf(buf, bufsize, "%d[", key->ncols);

	for (i = 0; i < key->ncols && len < bufsize; i++)
		len += snprintf(buf + len, bufsize - len, "%s%d", (i > 0) ? "," : "", (int) key->attnums[i]);

	if (len < bufsize)
		snprintf(buf + len, bufsize - len, "]");
}

/*
 * Record one sequential scan on a column set.  System catalogs are not
 * tracked.  If the table is full, the scan is silently not recorded.
 */
void
AutoIndexRecordScanColumns(Oid relid, const AttrNumber *attnums, int ncols)
{
	AutoIndexKey key;
	AutoIndexEntry *entry;
	bool found;
	uint64 scan_count;
	char colsbuf[32];

	if (!AutoIndexHash || !AutoIndexState || relid < FirstNormalObjectId || !AutoIndexKeyValid(relid, attnums, ncols))
		return;

	AutoIndexMakeKey(relid, attnums, ncols, &key);

	LWLockAcquire(&AutoIndexState->lock, LW_EXCLUSIVE);

	entry = (AutoIndexEntry *) hash_search(AutoIndexHash, &key, HASH_ENTER_NULL, &found);

	if (entry == NULL)
	{
		LWLockRelease(&AutoIndexState->lock);
		return;
	}

	if (!found)
	{
		/* entry may be recycled; clear non-key fields */
		MemSet(&entry->scan_count, 0, sizeof(*entry) - offsetof(AutoIndexEntry, scan_count));
		entry->scan_count = 1;
	}
	else
		entry->scan_count++;

	scan_count = entry->scan_count;

	LWLockRelease(&AutoIndexState->lock);

	AutoIndexFormatColumns(&key, colsbuf, sizeof(colsbuf));

	elog(LOG, "AUTO-INDEX (Shmem): relation_oid=%u columns=%s scan_count=%lu", relid, colsbuf, (unsigned long) scan_count);
}

void
AutoIndexRecordScan(Oid relid, AttrNumber attnum)
{
	AutoIndexRecordScanColumns(relid, &attnum, 1);
}

uint64
AutoIndexGetScanCountColumns(Oid relid, const AttrNumber *attnums, int ncols)
{
	AutoIndexKey key;
	AutoIndexEntry *entry;
	uint64 count = 0;

	if (!AutoIndexHash || !AutoIndexState || !AutoIndexKeyValid(relid, attnums, ncols))
		return 0;

	AutoIndexMakeKey(relid, attnums, ncols, &key);

	LWLockAcquire(&AutoIndexState->lock, LW_SHARED);

	entry = (AutoIndexEntry *) hash_search(AutoIndexHash, &key, HASH_FIND, NULL);
	if (entry)
		count = entry->scan_count;

	LWLockRelease(&AutoIndexState->lock);

	return count;
}

uint64
AutoIndexGetScanCount(Oid relid, AttrNumber attnum)
{
	return AutoIndexGetScanCountColumns(relid, &attnum, 1);
}

/* Remove a candidate, e.g. after its index has been built. */
void
AutoIndexResetScanColumns(Oid relid, const AttrNumber *attnums, int ncols)
{
	AutoIndexKey key;

	if (!AutoIndexHash || !AutoIndexState || !AutoIndexKeyValid(relid, attnums, ncols))
		return;

	AutoIndexMakeKey(relid, attnums, ncols, &key);

	LWLockAcquire(&AutoIndexState->lock, LW_EXCLUSIVE);

	hash_search(AutoIndexHash, &key, HASH_REMOVE, NULL);

	LWLockRelease(&AutoIndexState->lock);
}

void
AutoIndexResetScan(Oid relid, AttrNumber attnum)
{
	AutoIndexResetScanColumns(relid, &attnum, 1);
}

/*
 * Merge sorted column stats into one existing entry.  Caller holds the
 * lock.  Does nothing if the candidate has been removed.
 */
static void
AutoIndexMergeIntoEntry(const AutoIndexKey *key, uint64 nrows, int ncols, const AutoIndexColumnStats *sorted_stats)
{
	AutoIndexEntry *entry;
	int i;

	entry = (AutoIndexEntry *) hash_search(AutoIndexHash, key, HASH_FIND, NULL);

	if (entry == NULL)
		return;

	entry->rows_total += nrows;

	if (nrows > entry->rows_max)
		entry->rows_max = nrows;

	for (i = 0; i < ncols; i++)
	{
		AutoIndexColumnStats *dst = &entry->cols[i];
		const AutoIndexColumnStats *src = &sorted_stats[i];

		dst->nulls += src->nulls;
		dst->ndistinct = Max(dst->ndistinct, src->ndistinct);
		dst->overflow |= src->overflow;
		dst->dominant_frac = Max(dst->dominant_frac, src->dominant_frac);
		dst->width_sum += src->width_sum;
		dst->width_cnt += src->width_cnt;

		if (src->has_correlation)
		{
			dst->correlation = src->correlation;
			dst->has_correlation = true;
		}
	}
}

/*
 * Merge the column stats of one finished scan into its candidate.  For a
 * multi-column set, each column's stats also go to its single-column
 * candidate.
 */
void
AutoIndexMergeScanStats(Oid relid, uint64 nrows, const AttrNumber *attnums, const AutoIndexColumnStats *stats, int ncols)
{
	AutoIndexKey key;
	AttrNumber sorted[AUTO_INDEX_MAX_COLUMNS];
	AutoIndexColumnStats sorted_stats[AUTO_INDEX_MAX_COLUMNS];
	int i, j;

	if (!AutoIndexHash || !AutoIndexState || !AutoIndexKeyValid(relid, attnums, ncols) || nrows < 1 || stats == NULL)
		return;

	/* sort by attnum, keeping stats aligned */
	memcpy(sorted, attnums, ncols * sizeof(AttrNumber));
	memcpy(sorted_stats, stats, ncols * sizeof(AutoIndexColumnStats));

	for (i = 1; i < ncols; i++)
	{
		AttrNumber v = sorted[i];
		AutoIndexColumnStats s = sorted_stats[i];

		for (j = i - 1; j >= 0 && sorted[j] > v; j--)
		{
			sorted[j + 1] = sorted[j];
			sorted_stats[j + 1] = sorted_stats[j];
		}
		sorted[j + 1] = v;
		sorted_stats[j + 1] = s;
	}

	LWLockAcquire(&AutoIndexState->lock, LW_EXCLUSIVE);

	AutoIndexMakeKey(relid, sorted, ncols, &key);
	AutoIndexMergeIntoEntry(&key, nrows, ncols, sorted_stats);

	if (ncols > 1)
	{
		for (i = 0; i < ncols; i++)
		{
			AutoIndexKey single;

			AutoIndexMakeKey(relid, &sorted[i], 1, &single);
			AutoIndexMergeIntoEntry(&single, nrows, 1, &sorted_stats[i]);
		}
	}

	LWLockRelease(&AutoIndexState->lock);
}

typedef struct AutoIndexRankCtx
{
	FmgrInfo *cmp_proc;
	Oid collation;
	const Datum *vals;
} AutoIndexRankCtx;

static int
AutoIndexRankCmp(const void *a, const void *b, void *arg)
{
	AutoIndexRankCtx *ctx = (AutoIndexRankCtx *) arg;
	int32 cmp;

	cmp = DatumGetInt32(FunctionCall2Coll(ctx->cmp_proc, ctx->collation, ctx->vals[*(const int *) a], ctx->vals[*(const int *) b]));

	return (cmp > 0) - (cmp < 0);
}

/*
 * Spearman rank correlation between physical order and value order of
 * n values.  Negative results are clamped to 0.  Returns -1 if it cannot
 * be computed.
 */
double
AutoIndexSpearmanCorrelation(FmgrInfo *cmp_proc, Oid collation, const Datum *vals, int n)
{
	AutoIndexRankCtx ctx;
	int *order;
	double mean_phys = 0.0;
	double mean_rank;
	double num = 0.0;
	double denx = 0.0;
	double deny = 0.0;
	double corr;
	int k;

	if (cmp_proc == NULL || !OidIsValid(cmp_proc->fn_oid) || vals == NULL || n < 2)
		return -1.0;

	ctx.cmp_proc = cmp_proc;
	ctx.collation = collation;
	ctx.vals = vals;

	order = (int *) palloc(sizeof(int) * n);

	for (k = 0; k < n; k++)
		order[k] = k;

	qsort_arg(order, n, sizeof(int), AutoIndexRankCmp, &ctx);

	for (k = 0; k < n; k++)
		mean_phys += (double) order[k];

	mean_phys /= (double) n;
	mean_rank = (n - 1) / 2.0;

	for (k = 0; k < n; k++)
	{
		double dx = (double) order[k] - mean_phys;
		double dy = (double) k - mean_rank;

		num += dx * dy;
		denx += dx * dx;
		deny += dy * dy;
	}

	pfree(order);

	if (denx <= 0.0 || deny <= 0.0)
		return -1.0;

	corr = num / sqrt(denx * deny);

	if (corr < 0.0)
		corr = 0.0;

	if (corr > 1.0)
		corr = 1.0;

	return corr;
}

/* Copy all candidates into dest for evaluation by the worker. */
int
AutoIndexGetCandidatesSnapshot(AutoIndexSnapshot *dest, int max_entries)
{
	HASH_SEQ_STATUS status;
	AutoIndexEntry *entry;
	int count = 0;

	if (!AutoIndexHash || !AutoIndexState || !dest || max_entries <= 0)
		return 0;

	LWLockAcquire(&AutoIndexState->lock, LW_SHARED);

	hash_seq_init(&status, AutoIndexHash);
	while ((entry = (AutoIndexEntry *) hash_seq_search(&status)) != NULL)
	{
		if (count >= max_entries)
		{
			hash_seq_term(&status);
			break;
		}

		dest[count].datid = entry->key.datid;
		dest[count].relid = entry->key.relid;
		dest[count].ncols = entry->key.ncols;
		memset(dest[count].attnums, 0, sizeof(dest[count].attnums));
		memcpy(dest[count].attnums, entry->key.attnums, entry->key.ncols * sizeof(AttrNumber));
		dest[count].scan_count = entry->scan_count;
		dest[count].rows_total = entry->rows_total;
		dest[count].rows_max = entry->rows_max;
		memcpy(dest[count].cols, entry->cols, sizeof(entry->cols));
		count++;
	}

	LWLockRelease(&AutoIndexState->lock);

	return count;
}

/* Remove candidates from other databases; OIDs are per-database. */
int
AutoIndexPruneForeignDatabases(void)
{
	HASH_SEQ_STATUS status;
	AutoIndexEntry *entry;
	int removed = 0;

	if (!AutoIndexHash || !AutoIndexState)
		return 0;

	LWLockAcquire(&AutoIndexState->lock, LW_EXCLUSIVE);

	hash_seq_init(&status, AutoIndexHash);
	while ((entry = (AutoIndexEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.datid != MyDatabaseId)
		{
			hash_search(AutoIndexHash, &entry->key, HASH_REMOVE, NULL);
			removed++;
		}
	}

	LWLockRelease(&AutoIndexState->lock);

	return removed;
}