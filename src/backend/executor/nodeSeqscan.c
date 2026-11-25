/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "common/hashfn.h"
#include "executor/execScan.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "fmgr.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "utils/lsyscache.h"
#include "storage/autoindex_shmem.h"

static TupleTableSlot *SeqNext(SeqScanState *node);

/*
 * Column set accumulated while walking an AND-conjunction.  A NULL set
 * pointer means "single-column recording only" (inside OR/NOT branches).
 */
typedef struct AutoIndexColSet
{
    AttrNumber cols[AUTO_INDEX_MAX_COLUMNS];
    int        ncols;
    bool       overflow;
} AutoIndexColSet;

/* Actual per-column statistics collection (defined below) */
typedef struct AutoIndexScanCollector AutoIndexScanCollector;
static void AutoIndexCollectTuple(AutoIndexScanCollector *coll,
								  TupleTableSlot *slot);
static AutoIndexScanCollector *AutoIndexCreateCollector(SeqScanState *scanstate,
														AutoIndexColSet *collect);
static void AutoIndexFlushScanStats(AutoIndexScanCollector *coll,
									SeqScanState *scanstate);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(node->ss.ss_currentRelation,
								   estate->es_snapshot,
								   0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
	{
		/*
		 * Statistics collection: every visible tuple the scan
		 * evaluates feeds the per-column aggregates of its tracked
		 * columns.
		 */
		if (node->autoindex_stats != NULL)
			AutoIndexCollectTuple((AutoIndexScanCollector *) node->autoindex_stats,
								  slot);

		return slot;
	}
	return NULL;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple. This variant is used when there is no es_epq_active, no qual
 *		and no projection.  Passing const-NULLs for these to ExecScanExtended
 *		allows the compiler to eliminate the additional code that would
 *		ordinarily be required for the evaluation of these.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							NULL,
							NULL);
}

/*
 * Variant of ExecSeqScan() but when qual evaluation is required.
 */
static TupleTableSlot *
ExecSeqScanWithQual(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual != NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							pstate->qual,
							NULL);
}

/*
 * Variant of ExecSeqScan() but when projection is required.
 */
static TupleTableSlot *
ExecSeqScanWithProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	Assert(pstate->ps_ProjInfo != NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							NULL,
							pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan() but when qual evaluation and projection are
 * required.
 */
static TupleTableSlot *
ExecSeqScanWithQualProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual != NULL);
	Assert(pstate->ps_ProjInfo != NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							pstate->qual,
							pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan for when EPQ evaluation is required.  We don't
 * bother adding variants of this for with/without qual and projection as
 * EPQ doesn't seem as exciting a case to optimize for.
 */
static TupleTableSlot *
ExecSeqScanEPQ(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}

/*
 * Auto-index: detect filtered columns in a SeqScan.
 *
 * Each supported comparison records its column as a single-column
 * candidate.  Columns ANDed together at the top level are also recorded
 * as one multi-column candidate.
 */

/*
 * Add a column to a set, skipping duplicates.  A NULL set means we are
 * inside an OR/NOT branch and only single columns are recorded.
 * (AutoIndexColSet is defined above SeqNext.)
 */
static void
AutoIndexColSetAdd(AutoIndexColSet *set, AttrNumber attnum)
{
	int i;

	if (set == NULL)
		return;

	for (i = 0; i < set->ncols; i++)
	{
		if (set->cols[i] == attnum)
			return;
	}

	if (set->ncols >= AUTO_INDEX_MAX_COLUMNS)
	{
		set->overflow = true;
		return;
	}

	set->cols[set->ncols++] = attnum;
}

/* Record a column of the scanned table and add it to both sets. */
static void
AutoIndexNoteColumn(SeqScanState *scanstate, Var *var, AutoIndexColSet *set, AutoIndexColSet *collect)
{
	SeqScan *seqscan = castNode(SeqScan, scanstate->ss.ps.plan);

	/* ignore columns of other relations (joins) */
	if (var->varno != seqscan->scan.scanrelid)
		return;

	AutoIndexRecordScan(RelationGetRelid(scanstate->ss.ss_currentRelation), var->varattno);
	AutoIndexColSetAdd(set, var->varattno);
	AutoIndexColSetAdd(collect, var->varattno);
}

static void
DetectComparisonInNode(Node *node, SeqScanState *scanstate, AutoIndexColSet *set, AutoIndexColSet *collect)
{
	ListCell *lc;
	char *opname;
	bool is_supported;
	Node *left;
	Node *right;

	if (node == NULL)
		return;

	if (IsA(node, BoolExpr))
	{
		BoolExpr *expr = (BoolExpr *) node;

		/* only AND can use a multi-column index; OR/NOT columns are recorded alone */
		foreach(lc, expr->args)
			DetectComparisonInNode((Node *) lfirst(lc), scanstate, expr->boolop == AND_EXPR ? set : NULL, collect);

		return;
	}

	/* IS NULL / IS NOT NULL */
	if (IsA(node, NullTest))
	{
		NullTest *nt = (NullTest *) node;

		if (IsA(nt->arg, Var))
			AutoIndexNoteColumn(scanstate, (Var *) nt->arg, set, collect);

		return;
	}

	/* IN / NOT IN */
	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) node;

		opname = get_opname(saop->opno);
		if (opname == NULL)
			return;

		is_supported = strcmp(opname, "=") == 0 || strcmp(opname, "<>") == 0;
		pfree(opname);

		if (!is_supported || list_length(saop->args) != 2)
			return;

		left = (Node *) linitial(saop->args);
		if (IsA(left, Var))
			AutoIndexNoteColumn(scanstate, (Var *) left, set, collect);

		return;
	}

	/* ordinary comparisons and LIKE */
	if (IsA(node, OpExpr))
	{
		OpExpr *op = (OpExpr *) node;

		if (list_length(op->args) != 2)
			return;

		opname = get_opname(op->opno);
		if (opname == NULL)
			return;

		is_supported = strcmp(opname, "=") == 0 || strcmp(opname, "<") == 0 || strcmp(opname, "<=") == 0 || strcmp(opname, ">") == 0 || strcmp(opname, ">=") == 0 || strcmp(opname, "<>") == 0 || strcmp(opname, "!=") == 0 || strcmp(opname, "~~") == 0 || strcmp(opname, "~~*") == 0 || strcmp(opname, "!~~") == 0 || strcmp(opname, "!~~*") == 0;
		pfree(opname);

		if (!is_supported)
			return;

		left = (Node *) linitial(op->args);
		right = (Node *) lsecond(op->args);

		if (IsA(left, Var))
			AutoIndexNoteColumn(scanstate, (Var *) left, set, collect);
		else if (IsA(right, Var))
			AutoIndexNoteColumn(scanstate, (Var *) right, set, collect);
	}
}

/*
 * Walk the scan's quals.  "collect" gets every recorded column (including
 * OR/NOT branches) and decides which columns we gather stats for.
 */
static void
DetectComparisonPredicate(SeqScanState *scanstate, AutoIndexColSet *collect)
{
	ListCell *lc;
	SeqScan *seqscan = castNode(SeqScan, scanstate->ss.ps.plan);
	Oid relid = RelationGetRelid(scanstate->ss.ss_currentRelation);
	AutoIndexColSet set;

	collect->ncols = 0;
	collect->overflow = false;

	/* the qual list is implicitly ANDed, so all items share one set */
	set.ncols = 0;
	set.overflow = false;

	foreach(lc, seqscan->scan.plan.qual)
		DetectComparisonInNode((Node *) lfirst(lc), scanstate, &set, collect);

	if (!set.overflow && set.ncols >= 2)
		AutoIndexRecordScanColumns(relid, set.cols, set.ncols);
}

/*
 * Statistics collection.
 *
 * While the scan runs we look at each visible tuple and, for every tracked
 * column, count NULLs, track distinct values and their frequencies (up to
 * AUTO_INDEX_DISTINCT_CAP), sum widths and keep the first values in
 * physical order for the correlation.  Everything is backend-local and is
 * merged into shared memory once when the scan ends.
 */

typedef struct AutoIndexDistinctKey
{
	uint32 hash;		/* hash of the value's contents */
	Datum value;		/* by-ref types point to a private copy */
} AutoIndexDistinctKey;

typedef struct AutoIndexDistinctEntry
{
	AutoIndexDistinctKey key;
	uint64 count;
} AutoIndexDistinctEntry;

/* per-column state for the running scan */
typedef struct AutoIndexScanCol
{
	AttrNumber attnum;
	bool can_analyze;	/* type has a btree comparison function */
	Oid collation;
	FmgrInfo *cmp_proc;
	bool typbyval;
	int16 typlen;
	HTAB *distinct;		/* value -> count */
	uint64 ndistinct;
	bool overflow;		/* more than AUTO_INDEX_DISTINCT_CAP values */
	uint64 nulls;
	uint64 width_sum;
	uint64 width_cnt;
	uint64 dom_count;	/* count of the most common value */
	Datum *corr_vals;	/* values in physical order, for correlation */
	int ncorr;
} AutoIndexScanCol;

/* collector for one SeqScan node; survives rescans */
typedef struct AutoIndexScanCollector
{
	MemoryContext cxt;
	uint64 rows;		/* visible tuples seen */
	int ncols;
	AutoIndexScanCol cols[AUTO_INDEX_MAX_COLUMNS];
} AutoIndexScanCollector;

/*
 * dynahash's match function takes no context argument, so the compare
 * function is set here right before each hash_search.
 */
static FmgrInfo *AutoIndexMatchCmp = NULL;
static Oid AutoIndexMatchCollation = InvalidOid;

static uint32
AutoIndexDistinctHash(const void *key, Size keysize)
{
	return ((const AutoIndexDistinctKey *) key)->hash;
}

static int
AutoIndexDistinctMatch(const void *key1, const void *key2, Size keysize)
{
	const AutoIndexDistinctKey *k1 = (const AutoIndexDistinctKey *) key1;
	const AutoIndexDistinctKey *k2 = (const AutoIndexDistinctKey *) key2;
	int32 cmp;

	if (k1->hash != k2->hash)
		return 1;

	cmp = DatumGetInt32(FunctionCall2Coll(AutoIndexMatchCmp, AutoIndexMatchCollation, k1->value, k2->value));

	return (cmp != 0);
}

/*
 * Hash a non-null value by its contents.  Externally toasted values are
 * detoasted first (into the current context) so equal values hash equally.
 */
static uint32
AutoIndexValueHash(AutoIndexScanCol *col, Datum val, struct varlena **detoasted_out)
{
	*detoasted_out = NULL;

	if (col->typbyval)
		return hash_any((const unsigned char *) &val, sizeof(Datum));

	if (col->typlen == -1)
	{
		struct varlena *v = (struct varlena *) DatumGetPointer(val);

		if (VARATT_IS_EXTERNAL(v))
		{
			struct varlena *copy = pg_detoast_datum_copy(v);

			*detoasted_out = copy;
			v = copy;
		}

		return hash_any((const unsigned char *) VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
	}

	return hash_any((const unsigned char *) DatumGetPointer(val), (col->typlen > 0) ? col->typlen : (int) strlen(DatumGetPointer(val)) + 1);
}

/* on-disk width of a non-null value */
static int
AutoIndexValueWidth(AutoIndexScanCol *col, Datum val)
{
	if (col->typbyval)
		return (col->typlen > 0) ? col->typlen : (int) sizeof(Datum);

	if (col->typlen == -1)
		return VARSIZE_ANY(DatumGetPointer(val));

	return (col->typlen > 0) ? col->typlen : (int) strlen(DatumGetPointer(val)) + 1;
}

/* copy a by-ref value into the collector's context */
static Datum
AutoIndexCopyValue(AutoIndexScanCollector *coll, AutoIndexScanCol *col, Datum val)
{
	MemoryContext old = MemoryContextSwitchTo(coll->cxt);
	int len = AutoIndexValueWidth(col, val);
	void *copy = palloc(len);

	memcpy(copy, DatumGetPointer(val), len);
	MemoryContextSwitchTo(old);

	return PointerGetDatum(copy);
}

/*
 * Called from SeqNext for every visible tuple of a scan with tracked
 * columns.  Cost is one slot_getattr and one hash lookup per column.
 */
static void
AutoIndexCollectTuple(AutoIndexScanCollector *coll, TupleTableSlot *slot)
{
	int c;

	coll->rows++;

	for (c = 0; c < coll->ncols; c++)
	{
		AutoIndexScanCol *col = &coll->cols[c];
		bool isnull;
		Datum val;
		struct varlena *detoasted = NULL;
		AutoIndexDistinctKey key;
		AutoIndexDistinctEntry *hent;
		bool found;

		val = slot_getattr(slot, col->attnum, &isnull);

		if (isnull)
		{
			col->nulls++;
			continue;
		}

		col->width_sum += (uint64) AutoIndexValueWidth(col, val);
		col->width_cnt++;

		if (!col->can_analyze || col->overflow)
			continue;

		key.hash = AutoIndexValueHash(col, val, &detoasted);
		key.value = detoasted ? PointerGetDatum(detoasted) : val;

		AutoIndexMatchCmp = col->cmp_proc;
		AutoIndexMatchCollation = col->collation;

		hent = (AutoIndexDistinctEntry *) hash_search(col->distinct, &key, HASH_ENTER, &found);

		if (!found)
		{
			/* the slot's datum is only valid for this tuple, so keep a copy */
			if (!col->typbyval && detoasted == NULL)
				hent->key.value = AutoIndexCopyValue(coll, col, val);

			hent->count = 1;
			col->ndistinct++;

			if (col->ndistinct > AUTO_INDEX_DISTINCT_CAP)
				col->overflow = true;
		}
		else
			hent->count++;

		if (hent->count > col->dom_count)
			col->dom_count = hent->count;

		/* keep the first values in physical order for the correlation */
		if (col->ncorr < AUTO_INDEX_CORR_CAP)
		{
			Datum stable = val;

			if (!col->typbyval)
				stable = detoasted ? PointerGetDatum(detoasted) : AutoIndexCopyValue(coll, col, val);

			col->corr_vals[col->ncorr++] = stable;
		}
	}
}

/*
 * Set up the collector for this scan's columns.  Returns NULL if there is
 * nothing to track, so the per-tuple hook only costs a NULL check.
 */
static AutoIndexScanCollector *
AutoIndexCreateCollector(SeqScanState *scanstate, AutoIndexColSet *collect)
{
	AutoIndexScanCollector *coll;
	MemoryContext cxt;
	TupleDesc tupdesc;
	int c;

	if (collect->overflow || collect->ncols < 1)
		return NULL;

	cxt = AllocSetContextCreate(scanstate->ss.ps.state->es_query_cxt, "AutoIndex scan statistics", ALLOCSET_DEFAULT_SIZES);

	MemoryContextSwitchTo(cxt);

	coll = (AutoIndexScanCollector *) palloc0(sizeof(AutoIndexScanCollector));
	coll->cxt = cxt;
	coll->ncols = collect->ncols;

	tupdesc = RelationGetDescr(scanstate->ss.ss_currentRelation);

	for (c = 0; c < coll->ncols; c++)
	{
		AutoIndexScanCol *col = &coll->cols[c];
		const FormData_pg_attribute *att;
		TypeCacheEntry *typentry;
		HASHCTL info;

		col->attnum = collect->cols[c];

		/* bad attnum: drop this column and the rest */
		if (col->attnum < 1 || col->attnum > tupdesc->natts)
		{
			coll->ncols = c;
			break;
		}

		att = TupleDescAttr(tupdesc, col->attnum - 1);
		col->typbyval = att->attbyval;
		col->typlen = att->attlen;
		col->collation = att->attcollation;

		typentry = lookup_type_cache(att->atttypid, TYPECACHE_CMP_PROC_FINFO);
		if (typentry != NULL && OidIsValid(typentry->cmp_proc))
		{
			col->cmp_proc = &typentry->cmp_proc_finfo;
			col->can_analyze = true;
		}

		col->corr_vals = (Datum *) palloc(sizeof(Datum) * AUTO_INDEX_CORR_CAP);

		if (col->can_analyze)
		{
			memset(&info, 0, sizeof(info));
			info.keysize = sizeof(AutoIndexDistinctKey);
			info.entrysize = sizeof(AutoIndexDistinctEntry);
			info.hash = AutoIndexDistinctHash;
			info.match = AutoIndexDistinctMatch;
			info.hcxt = cxt;

			col->distinct = hash_create("AutoIndex distinct values", 256, &info, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
		}
	}

	MemoryContextSwitchTo(scanstate->ss.ps.state->es_query_cxt);

	return coll;
}

/* Scan finished: merge the stats into shared memory and free the collector. */
static void
AutoIndexFlushScanStats(AutoIndexScanCollector *coll, SeqScanState *scanstate)
{
	AutoIndexColumnStats stats[AUTO_INDEX_MAX_COLUMNS];
	AttrNumber attnums[AUTO_INDEX_MAX_COLUMNS];
	Oid relid = RelationGetRelid(scanstate->ss.ss_currentRelation);
	int c;

	for (c = 0; c < coll->ncols; c++)
	{
		AutoIndexScanCol *col = &coll->cols[c];

		stats[c].nulls = col->nulls;
		stats[c].ndistinct = col->ndistinct;
		stats[c].overflow = col->overflow;
		stats[c].dominant_frac = (coll->rows > 0) ? (double) col->dom_count / (double) coll->rows : 0.0;
		stats[c].width_sum = col->width_sum;
		stats[c].width_cnt = col->width_cnt;
		stats[c].correlation = AutoIndexSpearmanCorrelation(col->cmp_proc, col->collation, col->corr_vals, col->ncorr);
		stats[c].has_correlation = (stats[c].correlation >= 0.0);

		attnums[c] = col->attnum;
	}

	AutoIndexMergeScanStats(relid, coll->rows, attnums, stats, coll->ncols);
	MemoryContextDelete(coll->cxt);
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scan.scanrelid,
							 eflags);

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation),
						  table_slot_callbacks(scanstate->ss.ss_currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

	{
		AutoIndexColSet collect;

		DetectComparisonPredicate(scanstate, &collect); //phase2

		/*
		 * Exact per-column statistics are gathered from the tuples this
		 * scan evaluates and merged into the shared candidate at scan
		 * end.  NULL when the qual contained no supported column.
		 */
		scanstate->autoindex_stats =
			(void *) AutoIndexCreateCollector(scanstate, &collect);
	}

	/*
	 * When EvalPlanQual() is not in use, assign ExecProcNode for this node
	 * based on the presence of qual and projection. Each ExecSeqScan*()
	 * variant is optimized for the specific combination of these conditions.
	 */
	if (scanstate->ss.ps.state->es_epq_active != NULL)
		scanstate->ss.ps.ExecProcNode = ExecSeqScanEPQ;
	else if (scanstate->ss.ps.qual == NULL)
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScan;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithProject;
	}
	else
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithQual;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithQualProject;
	}

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Scan finished: merge the exact per-column statistics collected from
	 * the evaluated tuples into the shared candidate entry, then release
	 * the collector.
	 */
	if (node->autoindex_stats != NULL)
	{
		AutoIndexFlushScanStats((AutoIndexScanCollector *) node->autoindex_stats,
								node);
		node->autoindex_stats = NULL;
	}

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		table_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	TableScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
												  estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
	table_parallelscan_initialize(node->ss.ss_currentRelation,
								  pscan,
								  estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	ParallelTableScanDesc pscan;

	pscan = node->ss.ss_currentScanDesc->rs_parallel;
	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelTableScanDesc pscan;

	pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}
