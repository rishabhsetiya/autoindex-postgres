/*-------------------------------------------------------------------------
 *
 * autoindex_cost.h
 *        Cost-benefit evaluation for auto-indexing, driven by
 *        statistics collected by nodeSeqscan.c from the tuples evaluated
 *        during query execution, and the live planner cost parameters
 *        (seq_page_cost, random_page_cost, cpu_*_cost).
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTOINDEX_COST_H
#define AUTOINDEX_COST_H

#include "postgres.h"
#include "storage/autoindex_shmem.h"

/*
 * Result of evaluating an automatic-index candidate.
 */
typedef enum AutoIndexDecision
{
    AUTO_INDEX_REJECT,
    AUTO_INDEX_WAIT,
    AUTO_INDEX_ELIGIBLE
} AutoIndexDecision;

/*
 * Detailed evaluation result.
 */
typedef struct AutoIndexEvaluation
{
    Oid                 relid;
    int                 ncols;
    AttrNumber          attnums[AUTO_INDEX_MAX_COLUMNS];
    uint64              scan_count;

    AutoIndexDecision   decision;

    /* Statistics-derived estimates */
    double              estimated_selectivity;      /* combined, per scan */
    double              estimated_seqscan_cost;     /* planner-style cost of
                                                     * one filtered seq scan */
    double              estimated_indexscan_cost;   /* planner-style cost of
                                                     * one hypothetical btree
                                                     * index scan */
    double              estimated_benefit_per_scan; /* seqscan - indexscan */
    double              estimated_benefit;          /* per-scan benefit times
                                                     * the measured scan count */
    double              estimated_creation_cost;    /* one-time index build
                                                     * cost estimate */
    double              estimated_index_pages;
    double              estimated_heap_pages;

    bool                has_statistics;
} AutoIndexEvaluation;

/*
 * Configurable parameter.
 */
extern double autoindex_max_selectivity;

/*
 * Evaluating one candidate obtained from the shared-memory snapshot.
 *
 * The decision compares the accumulated benefit of indexing (per-scan
 * saving x observed scan count) against the one-time index build cost.
 * All inputs are from real measurements: measured scan counts from shared
 * memory, the relation block count, and per-column statistics
 * (null fraction, distinct values, dominant frequency, width,
 * physical correlation) that nodeSeqscan collected from every tuple the
 * completed sequential scans evaluated.
 *
 * The caller must be in a transaction with an active snapshot.
 */
extern AutoIndexDecision AutoIndexEvaluateCandidate(
    const AutoIndexSnapshot *candidate,
    AutoIndexEvaluation *result
);

#endif                          /* AUTOINDEX_COST_H */