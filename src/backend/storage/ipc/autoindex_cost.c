/*-------------------------------------------------------------------------
 *
 * autoindex_cost.c
 *	  Cost-benefit evaluation for auto-index candidates.
 *
 * Uses the column statistics gathered during sequential scans and the
 * planner's cost parameters.  An index is worth building once the saving
 * per scan, times the number of scans seen, exceeds the cost of building it.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "access/itup.h"
#include "access/table.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "storage/autoindex_cost.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

/* GUC: candidates matching more than this fraction of rows are rejected */
double autoindex_max_selectivity = 0.05;

/* BTREE_DEFAULT_FILLFACTOR */
#define AUTO_INDEX_BTREE_FILLFACTOR 0.90

/* DEFAULT_EQ_SEL in selfuncs.c */
#define AUTOINDEX_DEFAULT_EQ_SEL 0.005

/* Format a column list for logging, e.g. "2[1,3]". */
static void
AutoIndexFormatColumns(const AutoIndexSnapshot *candidate, char *buf, Size bufsize)
{
	int i;
	Size len;

	len = snprintf(buf, bufsize, "%d[", candidate->ncols);

	for (i = 0; i < candidate->ncols && len < bufsize; i++)
		len += snprintf(buf + len, bufsize - len, "%s%d", (i > 0) ? "," : "", (int) candidate->attnums[i]);

	if (len < bufsize)
		snprintf(buf + len, bufsize - len, "]");
}

/* Estimate how many index tuples fit on one btree leaf page. */
static double
AutoIndexBtreeEntriesPerPage(double avg_entry_width)
{
	double usable = (double) BLCKSZ - (double) SizeOfPageHeaderData;
	double entry_size = (double) (MAXALIGN(sizeof(IndexTupleData) + (Size) avg_entry_width) + sizeof(ItemIdData));
	double entries_per_page;

	entries_per_page = floor(usable * AUTO_INDEX_BTREE_FILLFACTOR / entry_size);

	if (entries_per_page < 1.0)
		entries_per_page = 1.0;

	return entries_per_page;
}

/* Heap pages fetched for a number of tuples (Mackert-Lohman, as in index_pages_fetched). */
static double
AutoIndexHeapPagesFetched(double table_pages, double tuples_fetched)
{
	double pages_fetched;
	double T;

	T = (table_pages > 1.0) ? table_pages : 1.0;

	if (tuples_fetched <= 1.0)
		return Min(tuples_fetched, T);

	pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);

	if (pages_fetched >= T)
		pages_fetched = T;
	else
		pages_fetched = ceil(pages_fetched);

	return pages_fetched;
}

/*
 * Evaluate one candidate.
 *
 * REJECT:   selectivity is above autoindex_max_selectivity.
 * WAIT:     no stats yet, relation missing, or not enough benefit yet.
 * ELIGIBLE: accumulated benefit covers the build cost.
 */
AutoIndexDecision
AutoIndexEvaluateCandidate(const AutoIndexSnapshot *candidate, AutoIndexEvaluation *result)
{
	Relation rel;
	double reltuples = 0.0;
	double relpages = 0.0;
	double combined_selectivity = 1.0;
	double avg_width_sum = 0.0;
	double correlation = -1.0;
	double entries_per_page;
	double index_pages;
	double depth;
	double selected;
	double heap_pages;
	double heap_page_cost;
	double seqscan_cost;
	double indexscan_cost;
	double benefit_per_scan;
	double accumulated_benefit;
	double creation_cost;
	char colsbuf[32];
	int i;

	if (result == NULL)
		return AUTO_INDEX_REJECT;

	memset(result, 0, sizeof(*result));

	result->decision = AUTO_INDEX_REJECT;

	if (candidate == NULL || !OidIsValid(candidate->relid) || candidate->ncols < 1 || candidate->ncols > AUTO_INDEX_MAX_COLUMNS)
		return result->decision;

	result->relid = candidate->relid;
	result->ncols = candidate->ncols;
	memcpy(result->attnums, candidate->attnums, sizeof(result->attnums));
	result->scan_count = candidate->scan_count;

	AutoIndexFormatColumns(candidate, colsbuf, sizeof(colsbuf));

	/* no finished scan has reported stats yet */
	if (candidate->rows_max < 1)
	{
		result->decision = AUTO_INDEX_WAIT;
		result->has_statistics = false;

		elog(LOG, "AUTO-INDEX: relation_oid=%u columns=%s decision=WAIT reason=no_scan_stats", candidate->relid, colsbuf);

		return result->decision;
	}

	/* relation may have been dropped; the worker cleans it up later */
	rel = try_table_open(candidate->relid, AccessShareLock);

	if (rel == NULL)
	{
		result->decision = AUTO_INDEX_WAIT;
		result->has_statistics = false;

		elog(LOG, "AUTO-INDEX: relation_oid=%u columns=%s decision=WAIT reason=no_relation", candidate->relid, colsbuf);

		return result->decision;
	}

	relpages = (double) RelationGetNumberOfBlocks(rel);

	table_close(rel, AccessShareLock);

	/* row count of the largest scan seen */
	reltuples = (double) candidate->rows_max;
	result->has_statistics = true;

	/*
	 * Per-column equality selectivity.  We don't know the actual value
	 * queried, so use the frequency of the most common value as a
	 * pessimistic estimate.
	 */
	for (i = 0; i < candidate->ncols; i++)
	{
		const AutoIndexColumnStats *col = &candidate->cols[i];
		double sel;

		if (col->overflow)
		{
			/* too many distinct values to track: treat as near-unique */
			sel = 1.0 / Max(reltuples, 1.0);

			if (col->dominant_frac > sel)
				sel = col->dominant_frac;
		}
		else if (col->width_cnt == 0)
		{
			/* all NULL */
			sel = 1e-10;
		}
		else if (col->ndistinct > 0)
		{
			sel = col->dominant_frac;
		}
		else
		{
			/* type has no btree comparison */
			sel = AUTOINDEX_DEFAULT_EQ_SEL;
		}

		if (sel < 1e-10)
			sel = 1e-10;

		if (sel > 1.0)
			sel = 1.0;

		/* assume columns are independent, like the planner */
		combined_selectivity *= sel;

		if (col->width_cnt > 0)
			avg_width_sum += (double) col->width_sum / (double) col->width_cnt;
		else
			avg_width_sum += (double) sizeof(Datum);

		/* use the leading column's correlation */
		if (correlation < 0.0 && col->has_correlation)
			correlation = col->correlation;
	}

	result->estimated_selectivity = combined_selectivity;

	if (combined_selectivity > autoindex_max_selectivity)
	{
		result->decision = AUTO_INDEX_REJECT;

		elog(LOG, "AUTO-INDEX: relation_oid=%u columns=%s decision=REJECT selectivity=%.6f threshold=%.6f", candidate->relid, colsbuf, combined_selectivity, autoindex_max_selectivity);

		return result->decision;
	}

	reltuples = Max(reltuples, 1.0);
	relpages = Max(relpages, 1.0);

	/* size of the hypothetical btree */
	entries_per_page = AutoIndexBtreeEntriesPerPage(avg_width_sum);
	index_pages = ceil(reltuples / entries_per_page);

	if (index_pages < 1.0)
		index_pages = 1.0;

	/* tree depth, charged as random page reads */
	depth = 1.0;

	if (reltuples > entries_per_page && entries_per_page >= 2.0)
		depth = ceil(log(reltuples) / log(entries_per_page));

	selected = Max(reltuples * combined_selectivity, 1.0);

	heap_pages = AutoIndexHeapPagesFetched(relpages, selected);

	/* interpolate heap page cost by correlation squared, similar to btcostestimate */
	if (correlation < 0.0)
		correlation = 0.0;

	heap_page_cost = correlation * correlation * seq_page_cost + (1.0 - correlation * correlation) * random_page_cost;

	seqscan_cost = seq_page_cost * relpages + cpu_tuple_cost * reltuples + cpu_operator_cost * (double) candidate->ncols * reltuples;

	indexscan_cost = depth * random_page_cost + heap_pages * heap_page_cost + cpu_index_tuple_cost * selected + cpu_operator_cost * (double) candidate->ncols * selected;

	benefit_per_scan = seqscan_cost - indexscan_cost;

	if (benefit_per_scan < 0.0)
		benefit_per_scan = 0.0;

	accumulated_benefit = benefit_per_scan * (double) candidate->scan_count;

	/* build cost: read heap, sort, write index */
	creation_cost = seq_page_cost * relpages + cpu_tuple_cost * reltuples + cpu_operator_cost * reltuples * (log(reltuples) / log(2.0)) + seq_page_cost * index_pages;

	result->estimated_seqscan_cost = seqscan_cost;
	result->estimated_indexscan_cost = indexscan_cost;
	result->estimated_benefit_per_scan = benefit_per_scan;
	result->estimated_benefit = accumulated_benefit;
	result->estimated_creation_cost = creation_cost;
	result->estimated_index_pages = index_pages;
	result->estimated_heap_pages = heap_pages;

	if (accumulated_benefit >= creation_cost)
	{
		result->decision = AUTO_INDEX_ELIGIBLE;

		elog(LOG, "AUTO-INDEX: relation_oid=%u columns=%s decision=ELIGIBLE selectivity=%.6f scans=%lu seqscan_cost=%.2f indexscan_cost=%.2f benefit_per_scan=%.2f accumulated_benefit=%.2f creation_cost=%.2f index_pages=%.0f heap_pages=%.0f", candidate->relid, colsbuf, combined_selectivity, (unsigned long) candidate->scan_count, seqscan_cost, indexscan_cost, benefit_per_scan, accumulated_benefit, creation_cost, index_pages, heap_pages);
	}
	else
	{
		result->decision = AUTO_INDEX_WAIT;

		elog(LOG, "AUTO-INDEX: relation_oid=%u columns=%s decision=WAIT selectivity=%.6f scans=%lu seqscan_cost=%.2f indexscan_cost=%.2f benefit_per_scan=%.2f accumulated_benefit=%.2f creation_cost=%.2f", candidate->relid, colsbuf, combined_selectivity, (unsigned long) candidate->scan_count, seqscan_cost, indexscan_cost, benefit_per_scan, accumulated_benefit, creation_cost);
	}

	return result->decision;
}