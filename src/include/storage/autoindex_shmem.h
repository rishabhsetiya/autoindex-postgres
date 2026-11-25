/*-------------------------------------------------------------------------
 *
 * autoindex_shmem.h
 *	  Shared memory hash table and concurrency control for tracking
 *	  sequential scans across all backends.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTOINDEX_SHMEM_H
#define AUTOINDEX_SHMEM_H

#include "postgres.h"
#include "access/attnum.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#define AUTO_INDEX_MAX_CANDIDATES 2048
#define AUTO_INDEX_MAX_COLUMNS 4

/*
 * Per-scan collection bounds (enforced by nodeSeqscan.c while the scan
 * runs; the resulting aggregates merged into shared memory are
 * over the rows actually evaluated).
 *
 * A column whose distinct-value set exceeds AUTO_INDEX_DISTINCT_CAP
 * within one scan is flagged and treated by the cost model as
 * near-unique.  Correlation is measured over the first
 * AUTO_INDEX_CORR_CAP rows of a scan in physical order.
 */
#define AUTO_INDEX_DISTINCT_CAP 65536
#define AUTO_INDEX_CORR_CAP 65536

/* column statistics merged from one sequential scan */
typedef struct AutoIndexColumnStats
{
	uint64		nulls;		/* rows where the column was NULL */
	uint64		ndistinct;	/* distinct non-null values observed */
	bool		overflow;	/* distinct set exceeded AUTO_INDEX_DISTINCT_CAP */
	double		dominant_frac;	/* most frequent value / rows in the scan */
	uint64		width_sum;	/* on-disk datum widths, summed */
	uint64		width_cnt;	/* non-null values contributing a width */
	double		correlation;	/* Spearman: value order vs physical order */
	bool		has_correlation;
} AutoIndexColumnStats;

/* Hash table key uniquely identifying a column set in a table */
typedef struct AutoIndexKey
{
	Oid			datid;		/* database OID: OIDs are per-database, so a
								 * candidate must never leak across databases */
	Oid			relid;		/* Table OID */
	int			ncols;		/* number of columns, 1..AUTO_INDEX_MAX_COLUMNS */
	AttrNumber	attnums[AUTO_INDEX_MAX_COLUMNS];	/* sorted ascending,
													 * unused slots zeroed */
} AutoIndexKey;

/* Hash table entry stored in shared memory */
typedef struct AutoIndexEntry
{
	AutoIndexKey	key;		/* hash key (must be first field in struct) */
	uint64			scan_count;	/* number of sequential scans observed */

	/*
	 * Exact statistics merged from completed sequential scans.  cols[]
	 * is aligned with key.attnums (canonical sorted order).
	 */
	uint64			rows_total;	/* tuples evaluated, summed over merges */
	uint64			rows_max;	/* tuples evaluated in the largest scan */
	AutoIndexColumnStats cols[AUTO_INDEX_MAX_COLUMNS];
} AutoIndexEntry;

/* Shared state containing the synchronization lock */
typedef struct AutoIndexSharedState
{
	LWLock		lock;
} AutoIndexSharedState;

/* Snapshot structure for background worker evaluation */
typedef struct AutoIndexSnapshot
{
	Oid			datid;
	Oid			relid;
	int			ncols;
	AttrNumber	attnums[AUTO_INDEX_MAX_COLUMNS];
	uint64		scan_count;

	/* exact scan-derived statistics, aligned with attnums[] */
	uint64		rows_total;
	uint64		rows_max;
	AutoIndexColumnStats cols[AUTO_INDEX_MAX_COLUMNS];
} AutoIndexSnapshot;

/* Public API declarations */
extern Size AutoIndexShmemSize(void);
extern void AutoIndexShmemInit(void);

/* Contract for Phase 2: Called by nodeSeqscan.c */
extern void AutoIndexRecordScan(Oid relid, AttrNumber attnum);
extern void AutoIndexRecordScanColumns(Oid relid, const AttrNumber *attnums, int ncols);

/*
 * Merge the exact per-column statistics gathered by one completed
 * sequential scan into the tracked candidate.  Rows cover all tuples the
 * scan evaluated; stats[] must describe the columns attnums[] in the
 * same order (any order is accepted; the merge canonicalizes).  If the
 * candidate was removed meanwhile (index built), the merge is skipped.
 */
extern void AutoIndexMergeScanStats(Oid relid, uint64 nrows, const AttrNumber *attnums, const AutoIndexColumnStats *stats, int ncols);

/* Contract for Phase 4/5: Called by Background Worker */
extern uint64 AutoIndexGetScanCount(Oid relid, AttrNumber attnum);
extern uint64 AutoIndexGetScanCountColumns(Oid relid, const AttrNumber *attnums, int ncols);
extern void   AutoIndexResetScan(Oid relid, AttrNumber attnum);
extern void   AutoIndexResetScanColumns(Oid relid, const AttrNumber *attnums, int ncols);
extern int    AutoIndexGetCandidatesSnapshot(AutoIndexSnapshot *dest, int max_entries);

/*
 * Remove every candidate that belongs to a database other than the
 * caller's (shared memory is cluster-wide, OIDs are per-database; a
 * foreign candidate can never be built by this worker and would only
 * occupy a slot).  Returns the number removed.
 */
extern int    AutoIndexPruneForeignDatabases(void);

/*
 * Spearman rank correlation between value order and physical (arrival)
 * order of the first n values, using the type's btree comparison
 * procedure.  Returns [0,1] (negative clamped to 0, as btcostestimate
 * does) or -1 when n < 2.  Used by nodeSeqscan.c at scan end.
 */
extern double AutoIndexSpearmanCorrelation(FmgrInfo *cmp_proc, Oid collation, const Datum *vals, int n);

#endif							/* AUTOINDEX_SHMEM_H */