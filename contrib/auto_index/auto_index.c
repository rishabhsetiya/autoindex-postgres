/*-------------------------------------------------------------------------
 *
 * auto_index.c
 *		Static background worker that periodically evaluates tracked
 *		index candidates and runs CREATE INDEX CONCURRENTLY for each
 *		eligible one directly (no intermediate queue table).
 *
 *
 * Load with:  shared_preload_libraries = 'auto_index'
 * GUCs:       auto_index.database  (postmaster-level, default 'postgres')
 *             auto_index.naptime   (seconds, reloadable, default 10)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/autoindex_cost.h"
#include "storage/autoindex_shmem.h"
#include "storage/latch.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

PGDLLEXPORT pg_noreturn void auto_index_worker_main(Datum main_arg);
void _PG_init(void);


/* GUCs */
static char *auto_index_database = NULL;
static int	auto_index_naptime = 10;

/* Long-lived context: survives the commits that auto_index performs internally. */
static MemoryContext worker_cxt = NULL;

/* One index to build, fully described by what the worker needs to run it. */
typedef struct auto_indexJob
{
	char	   *index_name;
	char	   *create_sql;
	char	   *drop_sql;		/* non-NULL: drop a leftover INVALID index first */
	Oid			relid;			/* for clearing the shmem candidate */
	int			nattnums;
	AttrNumber	attnums[AUTO_INDEX_MAX_COLUMNS];
} auto_indexJob;

typedef enum JobPrep
{
	JOB_STALE,					/* relation/column gone: drop candidate */
	JOB_ALREADY,				/* valid index already exists: drop candidate */
	JOB_SKIP,					/* cannot build now (name clash): back off */
	JOB_BUILD					/* job is filled in and ready to run */
} JobPrep;

/*
 * Failure back-off.
 * a small in-process table (worker-local, lost on restart) so a failed
 * candidate is not retried for FAILED_BACKOFF_SECS.
 */
#define FAILED_BACKOFF_SECS		3600
#define MAX_FAILED_TRACKED		64

typedef struct FailedEntry
{
	bool		used;
	Oid			relid;
	int			ncols;
	AttrNumber	attnums[AUTO_INDEX_MAX_COLUMNS];
	TimestampTz retry_at;
} FailedEntry;

static FailedEntry failed_tab[MAX_FAILED_TRACKED];
static int	failed_next = 0;

static FailedEntry *
find_failed(Oid relid, const AttrNumber *attnums, int ncols)
{
	int			i;

	for (i = 0; i < MAX_FAILED_TRACKED; i++)
	{
		FailedEntry *e = &failed_tab[i];

		if (e->used && e->relid == relid && e->ncols == ncols && memcmp(e->attnums, attnums, sizeof(AttrNumber) * ncols) == 0)
			return e;
	}
	return NULL;
}

static bool
candidate_in_backoff(Oid relid, const AttrNumber *attnums, int ncols)
{
	FailedEntry *e = find_failed(relid, attnums, ncols);

	if (e == NULL)
		return false;
	if (GetCurrentTimestamp() >= e->retry_at)
	{
		e->used = false;
		return false;
	}
	return true;
}

static void
remember_failure(Oid relid, const AttrNumber *attnums, int ncols)
{
	FailedEntry *e = find_failed(relid, attnums, ncols);

	if (e == NULL)
	{
		e = &failed_tab[failed_next];
		failed_next = (failed_next + 1) % MAX_FAILED_TRACKED;
	}

	e->used = true;
	e->relid = relid;
	e->ncols = ncols;
	memcpy(e->attnums, attnums, sizeof(AttrNumber) * ncols);
	e->retry_at = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), FAILED_BACKOFF_SECS * 1000L);
}

/*
 * Turn an eligible candidate into a ready-to-run job.  Must be called
 * inside a transaction; all returned strings live in worker_cxt.
 */
static JobPrep
prepare_job(const AutoIndexSnapshot *snap, auto_indexJob *job)
{
	MemoryContext old;
	char		relkind;
	Oid			nspoid;
	char	   *relname;
	char	   *nspname;
	StringInfoData name;
	StringInfoData cols;
	Oid			idxoid;
	bool		invalid_leftover = false;
	int			i;

	relkind = get_rel_relkind(snap->relid);
	if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
		return JOB_STALE;

	relname = get_rel_name(snap->relid);
	nspoid = get_rel_namespace(snap->relid);
	nspname = get_namespace_name(nspoid);
	if (relname == NULL || nspname == NULL)
		return JOB_STALE;

	old = MemoryContextSwitchTo(worker_cxt);

	initStringInfo(&name);
	initStringInfo(&cols);
	appendStringInfo(&name, "auto_idx_%s", relname);

	for (i = 0; i < snap->ncols; i++)
	{
		HeapTuple	atup;
		Form_pg_attribute att;

		atup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(snap->relid), Int16GetDatum(snap->attnums[i]));
		if (!HeapTupleIsValid(atup))
		{
			MemoryContextSwitchTo(old);
			return JOB_STALE;
		}
		att = (Form_pg_attribute) GETSTRUCT(atup);
		if (att->attisdropped)
		{
			ReleaseSysCache(atup);
			MemoryContextSwitchTo(old);
			return JOB_STALE;
		}

		appendStringInfo(&name, "_%s", NameStr(att->attname));
		appendStringInfo(&cols, "%s%s", i > 0 ? ", " : "", quote_identifier(NameStr(att->attname)));
		ReleaseSysCache(atup);
	}

	/* Same silent truncation CREATE INDEX would apply to a long name. */
	truncate_identifier(name.data, name.len, false);

	/* Is there already an index (or other relation) of that name? */
	idxoid = get_relname_relid(name.data, nspoid);
	if (OidIsValid(idxoid))
	{
		HeapTuple	itup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(idxoid));

		if (!HeapTupleIsValid(itup) || ((Form_pg_index) GETSTRUCT(itup))->indrelid != snap->relid)
		{
			if (HeapTupleIsValid(itup))
				ReleaseSysCache(itup);
			MemoryContextSwitchTo(old);
			elog(LOG, "auto_index poller: name \"%s\" already used by another relation, skipping", name.data);
			return JOB_SKIP;
		}

		if (((Form_pg_index) GETSTRUCT(itup))->indisvalid)
		{
			ReleaseSysCache(itup);
			MemoryContextSwitchTo(old);
			return JOB_ALREADY;
		}

		/* INVALID leftover from a failed concurrent build; clear it first */
		invalid_leftover = true;
		ReleaseSysCache(itup);
	}

	job->relid = snap->relid;
	job->nattnums = snap->ncols;
	memcpy(job->attnums, snap->attnums, sizeof(AttrNumber) * snap->ncols);
	job->index_name = pstrdup(name.data);
	job->create_sql = psprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s.%s USING btree (%s)", quote_identifier(name.data), quote_identifier(nspname), quote_identifier(relname), cols.data);
	job->drop_sql = invalid_leftover ? psprintf("DROP INDEX CONCURRENTLY IF EXISTS %s.%s", quote_identifier(nspname), quote_identifier(name.data)) : NULL;

	MemoryContextSwitchTo(old);
	return JOB_BUILD;
}

/*
 * Execute a single utility statement (CREATE/DROP INDEX CONCURRENTLY) as a
 * true top-level command.
 */
static void
run_utility_toplevel(const char *sql)
{
	List	   *parsetree_list;
	RawStmt    *raw;
	List	   *querytree_list;
	List	   *plantree_list;
	PlannedStmt *pstmt;
	QueryCompletion qc;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	pgstat_report_activity(STATE_RUNNING, sql);

	/* allocate trees outside the (soon to be recycled) transaction context */
	MemoryContextSwitchTo(worker_cxt);

	parsetree_list = pg_parse_query(sql);
	if (list_length(parsetree_list) != 1)
		elog(ERROR, "expected exactly one statement: %s", sql);

	raw = linitial_node(RawStmt, parsetree_list);
	querytree_list = pg_analyze_and_rewrite_fixedparams(raw, sql, NULL, 0, NULL);
	plantree_list = pg_plan_queries(querytree_list, sql, CURSOR_OPT_PARALLEL_OK, NULL);
	pstmt = linitial_node(PlannedStmt, plantree_list);


	PushActiveSnapshot(GetTransactionSnapshot());

	InitializeQueryCompletion(&qc);
	ProcessUtility(pstmt, sql, false, PROCESS_UTILITY_TOPLEVEL, NULL, NULL, None_Receiver, &qc);

	while (ActiveSnapshotSet())
		PopActiveSnapshot();

	CommitTransactionCommand();
	pgstat_report_stat(true);
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Build one index.  On success the shmem candidate is cleared so the
 * evaluator does not pick it again; on failure the error is logged and the
 * candidate is backed off for FAILED_BACKOFF_SECS.
 */
static void
build_index(auto_indexJob *job)
{
	volatile bool ok = true;
	char *volatile errtext = NULL;

	ereport(LOG, (errmsg("auto_index poller: building index \"%s\" on relation_oid=%u", job->index_name, job->relid)));

	PG_TRY();
	{
		if (job->drop_sql != NULL)
			run_utility_toplevel(job->drop_sql);

		run_utility_toplevel(job->create_sql);
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(worker_cxt);
		edata = CopyErrorData();
		FlushErrorState();
		AbortCurrentTransaction();

		ok = false;
		errtext = edata->message;
		pgstat_report_activity(STATE_IDLE, NULL);
	}
	PG_END_TRY();

	if (ok)
	{
		AutoIndexResetScanColumns(job->relid, job->attnums, job->nattnums);

		ereport(LOG, (errmsg("auto_index poller: built \"%s\"; cleared candidate relation_oid=%u columns=%d", job->index_name, job->relid, job->nattnums)));
	}
	else
	{
		remember_failure(job->relid, job->attnums, job->nattnums);

		ereport(LOG, (errmsg("auto_index poller: building \"%s\" failed: %s (will not retry for %d seconds)", job->index_name, errtext, FAILED_BACKOFF_SECS)));
	}
}

/*
 * Evaluate every tracked candidate (single- and multi-column) against the
 * real-statistics cost model, then build an index for each eligible one.
 * Evaluation and catalog checks share one transaction; the builds happen
 * afterwards, each as its own top-level command.
 */
static void
process_candidates(void)
{
	AutoIndexSnapshot  *snaps;
	AutoIndexEvaluation	eval;
	auto_indexJob	   *jobs;
	int					nsnaps;
	int					njobs = 0;
	int					i;

	MemoryContextSwitchTo(worker_cxt);
	MemoryContextReset(worker_cxt);

	snaps = (AutoIndexSnapshot *) MemoryContextAllocZero(worker_cxt, sizeof(AutoIndexSnapshot) * AUTO_INDEX_MAX_CANDIDATES);

	nsnaps = AutoIndexGetCandidatesSnapshot(snaps, AUTO_INDEX_MAX_CANDIDATES);
	if (nsnaps == 0)
		return;

	jobs = (auto_indexJob *) MemoryContextAllocZero(worker_cxt, sizeof(auto_indexJob) * nsnaps);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "evaluating auto-index candidates");

	for (i = 0; i < nsnaps; i++)
	{
		if (candidate_in_backoff(snaps[i].relid, snaps[i].attnums, snaps[i].ncols))
			continue;

		if (AutoIndexEvaluateCandidate(&snaps[i], &eval) != AUTO_INDEX_ELIGIBLE)
			continue;

		switch (prepare_job(&snaps[i], &jobs[njobs]))
		{
			case JOB_STALE:
				elog(LOG, "auto_index poller: dropping stale candidate relation_oid=%u columns=%d", snaps[i].relid, snaps[i].ncols);
				AutoIndexResetScanColumns(snaps[i].relid, snaps[i].attnums, snaps[i].ncols);
				break;

			case JOB_ALREADY:
				AutoIndexResetScanColumns(snaps[i].relid, snaps[i].attnums, snaps[i].ncols);
				break;

			case JOB_SKIP:
				remember_failure(snaps[i].relid, snaps[i].attnums, snaps[i].ncols);
				break;

			case JOB_BUILD:
				ereport(LOG, (errmsg("auto_index poller: eligible candidate relation_oid=%u columns=%d scans=%lu", snaps[i].relid, snaps[i].ncols, (unsigned long) snaps[i].scan_count)));
				njobs++;
				break;
		}
	}

	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(true);
	pgstat_report_activity(STATE_IDLE, NULL);

	for (i = 0; i < njobs; i++)
	{
		build_index(&jobs[i]);
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Worker entry point
 */
void
auto_index_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);		/* lets a long build be interrupted at shutdown */
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(auto_index_database, NULL, 0);
	pgstat_report_appname("auto_index poller");

	worker_cxt = AllocSetContextCreate(TopMemoryContext, "auto_index worker", ALLOCSET_DEFAULT_SIZES);

	ereport(LOG, (errmsg("auto_index poller started, database \"%s\", naptime %ds", auto_index_database, auto_index_naptime)));

	for (;;)
	{
		int			rc;

		/*
		 * Shared memory is cluster-wide while OIDs are per-database:
		 * drop candidates recorded by other databases' backends before
		 * evaluating (this worker can only ever build in its own
		 * database).
		 */
		(void) AutoIndexPruneForeignDatabases();

		/* Evaluate tracked candidates and build every eligible one. */
		process_candidates();

		MemoryContextSwitchTo(worker_cxt);

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, auto_index_naptime * 1000L, PG_WAIT_EXTENSION);
		(void) rc;
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}
}

/*
 * Library load hook: define GUCs and register the worker with the postmaster
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("auto_index.database", "Database the index-build poller connects to.", NULL, &auto_index_database, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("auto_index.naptime", "Seconds between evaluation passes.", NULL, &auto_index_naptime, 10, 1, 86400, PGC_SIGHUP, GUC_UNIT_S, NULL, NULL, NULL);

	MarkGUCPrefixReserved("auto_index");

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;	/* seconds; BGW_NEVER_RESTART to disable */
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "auto_index");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "auto_index_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "auto_index poller");
	snprintf(worker.bgw_type, BGW_MAXLEN, "auto_index poller");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}