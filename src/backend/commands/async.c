/*-------------------------------------------------------------------------
 *
 * async.c
 *	  Asynchronous notification: NOTIFY, LISTEN, UNLISTEN
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/async.c
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Async Notification Model as of 9.0:
 *
 * 1. Multiple backends on same machine. Multiple backends listening on
 *	  several channels. (Channels are also called "conditions" in other
 *	  parts of the code.)
 *
 * 2. There is one central queue in disk-based storage (directory pg_notify/),
 *	  with actively-used pages mapped into shared memory by the slru.c module.
 *	  All notification messages are placed in the queue and later read out
 *	  by listening backends.
 *
 *	  There is no central knowledge of which backend listens on which channel;
 *	  every backend has its own list of interesting channels.
 *
 *	  Although there is only one queue, notifications are treated as being
 *	  database-local; this is done by including the sender's database OID
 *	  in each notification message.  Listening backends ignore messages
 *	  that don't match their database OID.  This is important because it
 *	  ensures senders and receivers have the same database encoding and won't
 *	  misinterpret non-ASCII text in the channel name or payload string.
 *
 *	  Since notifications are not expected to survive database crashes,
 *	  we can simply clean out the pg_notify data at any reboot, and there
 *	  is no need for WAL support or fsync'ing.
 *
 * 3. Every backend that is listening on at least one channel registers by
 *	  entering its PID into the array in AsyncQueueControl. It then scans all
 *	  incoming notifications in the central queue and first compares the
 *	  database OID of the notification with its own database OID and then
 *	  compares the notified channel with the list of channels that it listens
 *	  to. In case there is a match it delivers the notification event to its
 *	  frontend.  Non-matching events are simply skipped.
 *
 * 4. The NOTIFY statement (routine Async_Notify) stores the notification in
 *	  a backend-local list which will not be processed until transaction end.
 *
 *	  Duplicate notifications from the same transaction are sent out as one
 *	  notification only. This is done to save work when for example a trigger
 *	  on a 2 million row table fires a notification for each row that has been
 *	  changed. If the application needs to receive every single notification
 *	  that has been sent, it can easily add some unique string into the extra
 *	  payload parameter.
 *
 *	  When the transaction is ready to commit, PreCommit_Notify() adds the
 *	  pending notifications to the head of the queue. The head pointer of the
 *	  queue always points to the next free position and a position is just a
 *	  page number and the offset in that page. This is done before marking the
 *	  transaction as committed in clog. If we run into problems writing the
 *	  notifications, we can still call elog(ERROR, ...) and the transaction
 *	  will roll back.
 *
 *	  Once we have put all of the notifications into the queue, we return to
 *	  CommitTransaction() which will then do the actual transaction commit.
 *
 *	  After commit we are called another time (AtCommit_Notify()). Here we
 *	  make any actual updates to the effective listen state (listenChannels).
 *	  Then we signal any backends that may be interested in our messages
 *	  (including our own backend, if listening).  This is done by
 *	  SignalBackends(), which scans the list of listening backends and sends a
 *	  PROCSIG_NOTIFY_INTERRUPT signal to every listening backend (we don't
 *	  know which backend is listening on which channel so we must signal them
 *	  all).  We can exclude backends that are already up to date, though, and
 *	  we can also exclude backends that are in other databases (unless they
 *	  are way behind and should be kicked to make them advance their
 *	  pointers).
 *
 *	  Finally, after we are out of the transaction altogether and about to go
 *	  idle, we scan the queue for messages that need to be sent to our
 *	  frontend (which might be notifies from other backends, or self-notifies
 *	  from our own).  This step is not part of the CommitTransaction sequence
 *	  for two important reasons.  First, we could get errors while sending
 *	  data to our frontend, and it's really bad for errors to happen in
 *	  post-commit cleanup.  Second, in cases where a procedure issues commits
 *	  within a single frontend command, we don't want to send notifies to our
 *	  frontend until the command is done; but notifies to other backends
 *	  should go out immediately after each commit.
 *
 * 5. Upon receipt of a PROCSIG_NOTIFY_INTERRUPT signal, the signal handler
 *	  sets the process's latch, which triggers the event to be processed
 *	  immediately if this backend is idle (i.e., it is waiting for a frontend
 *	  command and is not within a transaction block. C.f.
 *	  ProcessClientReadInterrupt()).  Otherwise the handler may only set a
 *	  flag, which will cause the processing to occur just before we next go
 *	  idle.
 *
 *	  Inbound-notify processing consists of reading all of the notifications
 *	  that have arrived since scanning last time. We read every notification
 *	  until we reach either a notification from an uncommitted transaction or
 *	  the head pointer's position.
 *
 * 6. To limit disk space consumption, the tail pointer needs to be advanced
 *	  so that old pages can be truncated. This is relatively expensive
 *	  (notably, it requires an exclusive lock), so we don't want to do it
 *	  often. We make sending backends do this work if they advanced the queue
 *	  head into a new page, but only once every QUEUE_CLEANUP_DELAY pages.
 *
 * An application that listens on the same channel it notifies will get
 * NOTIFY messages for its own NOTIFYs.  These can be ignored, if not useful,
 * by comparing be_pid in the NOTIFY message to the application's own backend's
 * PID.  (As of FE/BE protocol 2.0, the backend's PID is provided to the
 * frontend during startup.)  The above design guarantees that notifies from
 * other backends will never be missed by ignoring self-notifies.
 *
 * The amount of shared memory used for notify management (notify_buffers)
 * can be varied without affecting anything but performance.  The maximum
 * amount of notification data that can be queued at one time is determined
 * by max_notify_queue_pages GUC.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <unistd.h>
#include <signal.h>

#include "access/parallel.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/xlogrecovery.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "commands/async.h"
#include "common/hashfn.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procsignal.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc_hooks.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "storage/fd.h"
#include <unistd.h>

/* Missing definitions for WAL-based notification system */
#define AsyncQueueEntryEmptySize ASYNC_QUEUE_ENTRY_SIZE
#define SLRU_PAGE_SIZE BLCKSZ
#define AsyncCtl NotifyCtl

/* WAL record types */
#define XLOG_ASYNC_NOTIFY_DATA	0x00

/*
 * WAL record for notification data (written in PreCommit_Notify)
 */
typedef struct xl_async_notify_data
{
	Oid			dbid;			/* database ID */
	TransactionId xid;			/* transaction ID */
	int32		srcPid;			/* source backend PID */
	uint32		nnotifications;	/* number of notifications */
	/* followed by serialized notification data */
} xl_async_notify_data;

#define SizeOfAsyncNotifyData	(offsetof(xl_async_notify_data, nnotifications) + sizeof(uint32))



/*
 * Maximum size of a NOTIFY payload, including terminating NULL.  This
 * must be kept small enough so that a notification message fits on one
 * SLRU page.  The magic fudge factor here is noncritical as long as it's
 * more than AsyncQueueEntryEmptySize --- we make it significantly bigger
 * than that, so changes in that data structure won't affect user-visible
 * restrictions.
 */
#define NOTIFY_PAYLOAD_MAX_LENGTH	(BLCKSZ - NAMEDATALEN - 128)

/*
 * AsyncQueueEntry is defined in commands/async.h as a compact metadata-only
 * structure; notification content is stored in WAL.
 */

/* Queue alignment is still needed for SLRU page management */
#define QUEUEALIGN(len)		INTALIGN(len)

/*
 * QueuePosition is a scalar entry index. Derive page and byte offset from the
 * index using the fixed AsyncQueueEntry size.
 */
typedef int64 QueuePosition;


#define ASYNC_ENTRY_SIZE		((int) sizeof(AsyncQueueEntry))
#define ASYNC_ENTRIES_PER_PAGE	(BLCKSZ / ASYNC_ENTRY_SIZE)
/*
 * One SLRU page must contain an integral number of compact entries. That is
 * required for the index→page/offset mapping below (division/modulo by the
 * per-page entry count) and for unambiguous page-boundary detection.
 *
 * AsyncQueueEntry is currently 16 bytes (Oid 4 + TransactionId 4 + XLogRecPtr
 * 8) with natural alignment and no padding. BLCKSZ (QUEUE_PAGESIZE) is always
 * a multiple of 1024, so this assertion holds for standard builds. If the
 * entry layout changes in the future, this compile-time check ensures we fail
 * early rather than producing incorrect indexing math at runtime.
 */
StaticAssertDecl(BLCKSZ % sizeof(AsyncQueueEntry) == 0,
				 "AsyncQueueEntry size must divide QUEUE_PAGESIZE");

#define QUEUE_POS_PAGE(x)		((x) / ASYNC_ENTRIES_PER_PAGE)
#define QUEUE_POS_OFFSET(x)		((int)(((x) % ASYNC_ENTRIES_PER_PAGE) * ASYNC_ENTRY_SIZE))

#define SET_QUEUE_POS(x,y,z) \
	do { \
		(x) = ((int64) (y)) * ASYNC_ENTRIES_PER_PAGE + ((z) / ASYNC_ENTRY_SIZE); \
	} while (0)

#define QUEUE_POS_EQUAL(x,y)		((x) == (y))

#define QUEUE_POS_IS_ZERO(x)		((x) == 0)

/* choose logically smaller/larger positions */
#define QUEUE_POS_MIN(x,y)		((x) <= (y) ? (x) : (y))
#define QUEUE_POS_MAX(x,y)		((x) >= (y) ? (x) : (y))

/*
 * Parameter determining how often we try to advance the tail pointer:
 * we do that after every QUEUE_CLEANUP_DELAY pages of NOTIFY data.  This is
 * also the distance by which a backend in another database needs to be
 * behind before we'll decide we need to wake it up to advance its pointer.
 *
 * Resist the temptation to make this really large.  While that would save
 * work in some places, it would add cost in others.  In particular, this
 * should likely be less than notify_buffers, to ensure that backends
 * catch up before the pages they'll need to read fall out of SLRU cache.
 */
#define QUEUE_CLEANUP_DELAY 4

/*
 * Struct describing a listening backend's status
 */
typedef struct QueueBackendStatus
{
	int32		pid;			/* either a PID or InvalidPid */
	Oid			dboid;			/* backend's database OID, or InvalidOid */
	ProcNumber	nextListener;	/* id of next listener, or INVALID_PROC_NUMBER */
	QueuePosition pos;			/* backend has read queue up to here */
} QueueBackendStatus;

/*
 * Shared memory state for LISTEN/NOTIFY (excluding its SLRU stuff)
 *
 * The AsyncQueueControl structure is protected by the NotifyQueueLock and
 * NotifyQueueTailLock.
 *
 * When holding NotifyQueueLock in SHARED mode, backends may only inspect
 * their own entries as well as the head and tail pointers. Consequently we
 * can allow a backend to update its own record while holding only SHARED lock
 * (since no other backend will inspect it).
 *
 * When holding NotifyQueueLock in EXCLUSIVE mode, backends can inspect the
 * entries of other backends and also change the head pointer. When holding
 * both NotifyQueueLock and NotifyQueueTailLock in EXCLUSIVE mode, backends
 * can change the tail pointers.
 *
 * SLRU buffer pool is divided in banks and bank wise SLRU lock is used as
 * the control lock for the pg_notify SLRU buffers.
 * In order to avoid deadlocks, whenever we need multiple locks, we first get
 * NotifyQueueTailLock, then NotifyQueueLock, and lastly SLRU bank lock.
 *
 * Each backend uses the backend[] array entry with index equal to its
 * ProcNumber.  We rely on this to make SendProcSignal fast.
 *
 * The backend[] array entries for actively-listening backends are threaded
 * together using firstListener and the nextListener links, so that we can
 * scan them without having to iterate over inactive entries.  We keep this
 * list in order by ProcNumber so that the scan is cache-friendly when there
 * are many active entries.
 */
typedef struct AsyncQueueControl
{
	QueuePosition head;			/* head points to the next free location */
	QueuePosition tail;			/* tail must be <= the queue position of every
								 * listening backend */
	int64		stopPage;		/* oldest unrecycled page; must be <=
								 * tail.page */
	int64		reservedEntries;	/* number of entries reserved pre-commit */
	ProcNumber	firstListener;	/* id of first listener, or
								 * INVALID_PROC_NUMBER */
	TimestampTz lastQueueFillWarn;	/* time of last queue-full msg */
	QueueBackendStatus backend[FLEXIBLE_ARRAY_MEMBER];
} AsyncQueueControl;

static AsyncQueueControl *asyncQueueControl;

#define QUEUE_HEAD					(asyncQueueControl->head)
#define QUEUE_TAIL					(asyncQueueControl->tail)
#define QUEUE_STOP_PAGE				(asyncQueueControl->stopPage)
#define QUEUE_FIRST_LISTENER		(asyncQueueControl->firstListener)
#define QUEUE_BACKEND_PID(i)		(asyncQueueControl->backend[i].pid)
#define QUEUE_BACKEND_DBOID(i)		(asyncQueueControl->backend[i].dboid)
#define QUEUE_NEXT_LISTENER(i)		(asyncQueueControl->backend[i].nextListener)
#define QUEUE_BACKEND_POS(i)		(asyncQueueControl->backend[i].pos)

/*
 * The SLRU buffer area through which we access the notification queue
 */
static SlruCtlData NotifyCtlData;

#define NotifyCtl					(&NotifyCtlData)
#define QUEUE_PAGESIZE				BLCKSZ

#define QUEUE_FULL_WARN_INTERVAL	5000	/* warn at most once every 5s */

/*
 * listenChannels identifies the channels we are actually listening to
 * (ie, have committed a LISTEN on).  It is a simple list of channel names,
 * allocated in TopMemoryContext.
 */
static List *listenChannels = NIL;	/* list of C strings */

/*
 * State for pending LISTEN/UNLISTEN actions consists of an ordered list of
 * all actions requested in the current transaction.  As explained above,
 * we don't actually change listenChannels until we reach transaction commit.
 *
 * The list is kept in CurTransactionContext.  In subtransactions, each
 * subtransaction has its own list in its own CurTransactionContext, but
 * successful subtransactions attach their lists to their parent's list.
 * Failed subtransactions simply discard their lists.
 */
typedef enum
{
	LISTEN_LISTEN,
	LISTEN_UNLISTEN,
	LISTEN_UNLISTEN_ALL,
} ListenActionKind;

typedef struct
{
	ListenActionKind action;
	char		channel[FLEXIBLE_ARRAY_MEMBER]; /* nul-terminated string */
} ListenAction;

typedef struct ActionList
{
	int			nestingLevel;	/* current transaction nesting depth */
	List	   *actions;		/* list of ListenAction structs */
	struct ActionList *upper;	/* details for upper transaction levels */
} ActionList;

static ActionList *pendingActions = NULL;

/*
 * State for outbound notifies consists of a list of all channels+payloads
 * NOTIFYed in the current transaction.  We do not actually perform a NOTIFY
 * until and unless the transaction commits.  pendingNotifies is NULL if no
 * NOTIFYs have been done in the current (sub) transaction.
 *
 * We discard duplicate notify events issued in the same transaction.
 * Hence, in addition to the list proper (which we need to track the order
 * of the events, since we guarantee to deliver them in order), we build a
 * hash table which we can probe to detect duplicates.  Since building the
 * hash table is somewhat expensive, we do so only once we have at least
 * MIN_HASHABLE_NOTIFIES events queued in the current (sub) transaction;
 * before that we just scan the events linearly.
 *
 * The list is kept in CurTransactionContext.  In subtransactions, each
 * subtransaction has its own list in its own CurTransactionContext, but
 * successful subtransactions add their entries to their parent's list.
 * Failed subtransactions simply discard their lists.  Since these lists
 * are independent, there may be notify events in a subtransaction's list
 * that duplicate events in some ancestor (sub) transaction; we get rid of
 * the dups when merging the subtransaction's list into its parent's.
 *
 * Note: the action and notify lists do not interact within a transaction.
 * In particular, if a transaction does NOTIFY and then LISTEN on the same
 * condition name, it will get a self-notify at commit.  This is a bit odd
 * but is consistent with our historical behavior.
 */
typedef struct Notification
{
	uint16		channel_len;	/* length of channel-name string */
	uint16		payload_len;	/* length of payload string */
	/* null-terminated channel name, then null-terminated payload follow */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} Notification;

typedef struct NotificationList
{
	int			nestingLevel;	/* current transaction nesting depth */
	List	   *events;			/* list of Notification structs */
	HTAB	   *hashtab;		/* hash of NotificationHash structs, or NULL */
	struct NotificationList *upper; /* details for upper transaction levels */
} NotificationList;

#define MIN_HASHABLE_NOTIFIES 16	/* threshold to build hashtab */

struct NotificationHash
{
	Notification *event;		/* => the actual Notification struct */
};

static NotificationList *pendingNotifies = NULL;

/*
 * Inbound notifications are initially processed by HandleNotifyInterrupt(),
 * called from inside a signal handler. That just sets the
 * notifyInterruptPending flag and sets the process
 * latch. ProcessNotifyInterrupt() will then be called whenever it's safe to
 * actually deal with the interrupt.
 */
volatile sig_atomic_t notifyInterruptPending = false;

/* True if we've registered an on_shmem_exit cleanup */
static bool unlistenExitRegistered = false;

/* True if we're currently registered as a listener in asyncQueueControl */
static bool amRegisteredListener = false;

/* have we advanced to a page that's a multiple of QUEUE_CLEANUP_DELAY? */
static bool tryAdvanceTail = false;
/* true if this backend reserved one compact entry pre-commit */
static bool notifyEntryReserved = false;

/* GUC parameters */
bool		Trace_notify = false;

/* For 8 KB pages this gives 8 GB of disk space */
int			max_notify_queue_pages = 1048576;

/* local function prototypes */
static inline int64 asyncQueuePageDiff(int64 p, int64 q);
static inline bool asyncQueuePagePrecedes(int64 p, int64 q);
static void queue_listen(ListenActionKind action, const char *channel);
static void Async_UnlistenOnExit(int code, Datum arg);
static void Exec_ListenPreCommit(void);
static void Exec_ListenCommit(const char *channel);
static void Exec_UnlistenCommit(const char *channel);
static void Exec_UnlistenAllCommit(void);
static bool IsListeningOn(const char *channel);
static void asyncQueueUnregister(void);
static bool asyncQueueAdvance(volatile QueuePosition *position, int entryLength);
static double asyncQueueUsage(void);
static void SignalBackends(void);
static void asyncQueueReadAllNotifications(void);
static bool asyncQueueProcessPageEntries(volatile QueuePosition *current,
										 QueuePosition stop,
										 char *page_buffer);
static void asyncQueueAdvanceTail(void);
static void ProcessIncomingNotify(bool flush);
static bool AsyncExistsPendingNotify(Notification *n);
static void AddEventToPendingNotifies(Notification *n);
static uint32 notification_hash(const void *key, Size keysize);
static int	notification_match(const void *key1, const void *key2, Size keysize);
static void ClearPendingActionsAndNotifies(void);
static void processNotificationFromWAL(XLogRecPtr notify_lsn);
/* prototype provided in commands/async.h */

/*
 * Per-page committed minimum notify LSNs. Indexed by page_no % max_notify_queue_pages
 * and tagged with the exact page_no to avoid modulo aliasing.
 */
typedef struct NotifyPageMinEntry
{
	int64	   page_no;	/* absolute queue page number or -1 if invalid */
	XLogRecPtr  min_lsn;	/* minimum notify_data_lsn for committed entries on page */
} NotifyPageMinEntry;

static NotifyPageMinEntry *NotifyPageMins = NULL; /* shmem array of length max_notify_queue_pages */

/*
 * Uncommitted NOTIFY tracker. One node per top-level xact that has emitted
 * NOTIFY WAL but not yet committed. This is kept small; use a fixed-size
 * array with at most MaxBackends entries. Protected by NotifyQueueLock.
 */
typedef struct UncommittedNotifyEntry
{
	FullTransactionId fxid;
	XLogRecPtr		write_lsn;
	bool			  in_use;
} UncommittedNotifyEntry;

static UncommittedNotifyEntry *UncommittedNotifies = NULL; /* shmem array [MaxBackends] */

/* Helpers to update per-page mins; caller must hold NotifyQueueLock. */
static inline void
NotifyPageMinUpdateForPage(int64 page_no, XLogRecPtr lsn)
{
	int idx;
	NotifyPageMinEntry *e;

	if (NotifyPageMins == NULL || page_no < 0)
		return;

	idx = (int) (page_no % max_notify_queue_pages);
	e = &NotifyPageMins[idx];
	if (e->page_no != page_no)
	{
		e->page_no = page_no;
		e->min_lsn = lsn;
	}
	else
	{
		if (XLogRecPtrIsInvalid(e->min_lsn) || (lsn < e->min_lsn))
			e->min_lsn = lsn;
	}
}

/* Invalidate [from_page, to_page) entries; caller must hold NotifyQueueLock. */
static inline void
NotifyPageMinInvalidateRange(int64 from_page, int64 to_page)
{
	int64 p;
	int idx;

	if (NotifyPageMins == NULL)
		return;
	for (p = from_page; p < to_page; p++)
	{
		idx = (int) (p % max_notify_queue_pages);
		if (NotifyPageMins[idx].page_no == p)
		{
			NotifyPageMins[idx].page_no = -1;
			NotifyPageMins[idx].min_lsn = InvalidXLogRecPtr;
		}
	}
}

/* Uncommitted list maintenance; protected by NotifyQueueLock. */
/* Internal helper: caller must hold NotifyQueueLock EXCLUSIVE */
/*
 * Add or update the uncommitted NOTIFY pin for a top-level transaction.
 * Caller must hold NotifyQueueLock EXCLUSIVE.
 */
static void
UncommittedNotifyAdd(FullTransactionId fxid, XLogRecPtr lsn)
{
	int free_slot = -1;
	int i;

	if (UncommittedNotifies == NULL || !FullTransactionIdIsValid(fxid))
		return;

	/* If already present (shouldn't happen), update; else insert in a free slot. */
	for (i = 0; i < MaxBackends; i++)
	{
		if (UncommittedNotifies[i].in_use)
		{
			if (FullTransactionIdEquals(UncommittedNotifies[i].fxid, fxid))
			{
				/* Keep the minimum write_lsn per top-level xact */
				if (XLogRecPtrIsInvalid(UncommittedNotifies[i].write_lsn) ||
					(lsn < UncommittedNotifies[i].write_lsn))
					UncommittedNotifies[i].write_lsn = lsn;
                return;
			}
		}
		else if (free_slot < 0)
			free_slot = i;
	}
	if (free_slot >= 0)
	{
		UncommittedNotifies[free_slot].fxid = fxid;
		UncommittedNotifies[free_slot].write_lsn = lsn;
		UncommittedNotifies[free_slot].in_use = true;
	}
	else
	{
		/* Extremely unlikely: fallback to no-op rather than ERROR. */
	}
}

/* wrapper removed: all call sites hold NotifyQueueLock already */

static void
UncommittedNotifyRemoveByFullXid(FullTransactionId fxid)
{
	if (UncommittedNotifies == NULL || !FullTransactionIdIsValid(fxid))
		return;
	for (int i = 0; i < MaxBackends; i++)
	{
		if (UncommittedNotifies[i].in_use && FullTransactionIdEquals(UncommittedNotifies[i].fxid, fxid))
		{
			UncommittedNotifies[i].in_use = false;
			UncommittedNotifies[i].fxid = InvalidFullTransactionId;
			UncommittedNotifies[i].write_lsn = InvalidXLogRecPtr;
            break;
		}
	}
}

/*
 * Compute the difference between two queue page numbers.
 * Previously this function accounted for a wraparound.
 */
static inline int64
asyncQueuePageDiff(int64 p, int64 q)
{
	return p - q;
}

/*
 * Determines whether p precedes q.
 * Previously this function accounted for a wraparound.
 */
static inline bool
asyncQueuePagePrecedes(int64 p, int64 q)
{
	return p < q;
}

/*
 * Report space needed for our shared memory area
 */
Size
AsyncShmemSize(void)
{
	Size		size;

	/* This had better match AsyncShmemInit */
	size = mul_size(MaxBackends, sizeof(QueueBackendStatus));
	size = add_size(size, offsetof(AsyncQueueControl, backend));

	size = add_size(size, SimpleLruShmemSize(notify_buffers, 0));

	/* Per-page committed mins */
	size = add_size(size, mul_size(max_notify_queue_pages, sizeof(NotifyPageMinEntry)));
	/* Uncommitted list (MaxBackends entries) */
	size = add_size(size, mul_size(MaxBackends, sizeof(UncommittedNotifyEntry)));

	return size;
}

/*
 * Initialize our shared memory area
 */
void
AsyncShmemInit(void)
{
	bool		found;
	Size		size;

	/*
	 * Create or attach to the AsyncQueueControl structure.
	 */
	size = mul_size(MaxBackends, sizeof(QueueBackendStatus));
	size = add_size(size, offsetof(AsyncQueueControl, backend));

	asyncQueueControl = (AsyncQueueControl *)
		ShmemInitStruct("Async Queue Control", size, &found);

	if (!found)
	{
		/* First time through, so initialize it */
		SET_QUEUE_POS(QUEUE_HEAD, 0, 0);
		SET_QUEUE_POS(QUEUE_TAIL, 0, 0);
		QUEUE_STOP_PAGE = 0;
		asyncQueueControl->reservedEntries = 0;
		QUEUE_FIRST_LISTENER = INVALID_PROC_NUMBER;
		asyncQueueControl->lastQueueFillWarn = 0;
		for (int i = 0; i < MaxBackends; i++)
		{
			QUEUE_BACKEND_PID(i) = InvalidPid;
			QUEUE_BACKEND_DBOID(i) = InvalidOid;
			QUEUE_NEXT_LISTENER(i) = INVALID_PROC_NUMBER;
			SET_QUEUE_POS(QUEUE_BACKEND_POS(i), 0, 0);
		}
	}

	/*
	 * Set up SLRU management of the pg_notify data. Note that long segment
	 * names are used in order to avoid wraparound.
	 */
	NotifyCtl->PagePrecedes = asyncQueuePagePrecedes;
	SimpleLruInit(NotifyCtl, "notify", notify_buffers, 0,
				  "pg_notify", LWTRANCHE_NOTIFY_BUFFER, LWTRANCHE_NOTIFY_SLRU,
				  SYNC_HANDLER_NONE, true);

	if (!found)
	{
		/*
		 * During start or reboot, clean out the pg_notify directory.
		 */
		(void) SlruScanDirectory(NotifyCtl, SlruScanDirCbDeleteAll, NULL);
	}

	/* Allocate/attach per-page committed mins array */
	{
		bool found2 = false;
		NotifyPageMins = (NotifyPageMinEntry *)
			ShmemInitStruct("Notify Per-Page Min Array",
							sizeof(NotifyPageMinEntry) * (Size) max_notify_queue_pages,
							&found2);
		if (!found2)
		{
			for (int i = 0; i < max_notify_queue_pages; i++)
			{
				NotifyPageMins[i].page_no = -1;
				NotifyPageMins[i].min_lsn = InvalidXLogRecPtr;
			}
		}
	}

	/* Allocate/attach uncommitted list */
	{
		bool found3 = false;
		UncommittedNotifies = (UncommittedNotifyEntry *)
			ShmemInitStruct("Notify Uncommitted List",
							sizeof(UncommittedNotifyEntry) * (Size) MaxBackends,
							&found3);
		if (!found3)
		{
			for (int i = 0; i < MaxBackends; i++)
			{
				UncommittedNotifies[i].fxid = InvalidFullTransactionId;
				UncommittedNotifies[i].write_lsn = InvalidXLogRecPtr;
				UncommittedNotifies[i].in_use = false;
			}
		}
	}
}


/*
 * pg_notify -
 *	  SQL function to send a notification event
 */
Datum
pg_notify(PG_FUNCTION_ARGS)
{
	const char *channel;
	const char *payload;

	if (PG_ARGISNULL(0))
		channel = "";
	else
		channel = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		payload = "";
	else
		payload = text_to_cstring(PG_GETARG_TEXT_PP(1));

	/* For NOTIFY as a statement, this is checked in ProcessUtility */
	PreventCommandDuringRecovery("NOTIFY");

	Async_Notify(channel, payload);

	PG_RETURN_VOID();
}


/*
 * Async_Notify
 *
 *		This is executed by the SQL notify command.
 *
 *		Adds the message to the list of pending notifies.
 *		Actual notification happens during transaction commit.
 *		^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 */
void
Async_Notify(const char *channel, const char *payload)
{
	int			my_level = GetCurrentTransactionNestLevel();
	size_t		channel_len;
	size_t		payload_len;
	Notification *n;
	MemoryContext oldcontext;

	if (IsParallelWorker())
		elog(ERROR, "cannot send notifications from a parallel worker");

	if (Trace_notify)
		elog(DEBUG1, "Async_Notify(%s)", channel);

	channel_len = channel ? strlen(channel) : 0;
	payload_len = payload ? strlen(payload) : 0;

	/* a channel name must be specified */
	if (channel_len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("channel name cannot be empty")));

	/* enforce length limits */
	if (channel_len >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("channel name too long")));

	if (payload_len >= NOTIFY_PAYLOAD_MAX_LENGTH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("payload string too long")));

	/*
	 * We must construct the Notification entry, even if we end up not using
	 * it, in order to compare it cheaply to existing list entries.
	 *
	 * The notification list needs to live until end of transaction, so store
	 * it in the transaction context.
	 */
	oldcontext = MemoryContextSwitchTo(CurTransactionContext);

	n = (Notification *) palloc(offsetof(Notification, data) +
								channel_len + payload_len + 2);
	n->channel_len = channel_len;
	n->payload_len = payload_len;
	strcpy(n->data, channel);
	if (payload)
		strcpy(n->data + channel_len + 1, payload);
	else
		n->data[channel_len + 1] = '\0';

	if (pendingNotifies == NULL || my_level > pendingNotifies->nestingLevel)
	{
		NotificationList *notifies;

		/*
		 * First notify event in current (sub)xact. Note that we allocate the
		 * NotificationList in TopTransactionContext; the nestingLevel might
		 * get changed later by AtSubCommit_Notify.
		 */
		notifies = (NotificationList *)
			MemoryContextAlloc(TopTransactionContext,
							   sizeof(NotificationList));
		notifies->nestingLevel = my_level;
		notifies->events = list_make1(n);
		/* We certainly don't need a hashtable yet */
		notifies->hashtab = NULL;
		notifies->upper = pendingNotifies;
		pendingNotifies = notifies;
	}
	else
	{
		/* Now check for duplicates */
		if (AsyncExistsPendingNotify(n))
		{
			/* It's a dup, so forget it */
			pfree(n);
			MemoryContextSwitchTo(oldcontext);
			return;
		}

		/* Append more events to existing list */
		AddEventToPendingNotifies(n);
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * queue_listen
 *		Common code for listen, unlisten, unlisten all commands.
 *
 *		Adds the request to the list of pending actions.
 *		Actual update of the listenChannels list happens during transaction
 *		commit.
 */
static void
queue_listen(ListenActionKind action, const char *channel)
{
	MemoryContext oldcontext;
	ListenAction *actrec;
	int			my_level = GetCurrentTransactionNestLevel();

	/*
	 * Unlike Async_Notify, we don't try to collapse out duplicates. It would
	 * be too complicated to ensure we get the right interactions of
	 * conflicting LISTEN/UNLISTEN/UNLISTEN_ALL, and it's unlikely that there
	 * would be any performance benefit anyway in sane applications.
	 */
	oldcontext = MemoryContextSwitchTo(CurTransactionContext);

	/* space for terminating null is included in sizeof(ListenAction) */
	actrec = (ListenAction *) palloc(offsetof(ListenAction, channel) +
									 strlen(channel) + 1);
	actrec->action = action;
	strcpy(actrec->channel, channel);

	if (pendingActions == NULL || my_level > pendingActions->nestingLevel)
	{
		ActionList *actions;

		/*
		 * First action in current sub(xact). Note that we allocate the
		 * ActionList in TopTransactionContext; the nestingLevel might get
		 * changed later by AtSubCommit_Notify.
		 */
		actions = (ActionList *)
			MemoryContextAlloc(TopTransactionContext, sizeof(ActionList));
		actions->nestingLevel = my_level;
		actions->actions = list_make1(actrec);
		actions->upper = pendingActions;
		pendingActions = actions;
	}
	else
		pendingActions->actions = lappend(pendingActions->actions, actrec);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Async_Listen
 *
 *		This is executed by the SQL listen command.
 */
void
Async_Listen(const char *channel)
{
	if (Trace_notify)
		elog(DEBUG1, "Async_Listen(%s,%d)", channel, MyProcPid);

	queue_listen(LISTEN_LISTEN, channel);
}

/*
 * Async_Unlisten
 *
 *		This is executed by the SQL unlisten command.
 */
void
Async_Unlisten(const char *channel)
{
	if (Trace_notify)
		elog(DEBUG1, "Async_Unlisten(%s,%d)", channel, MyProcPid);

	/* If we couldn't possibly be listening, no need to queue anything */
	if (pendingActions == NULL && !unlistenExitRegistered)
		return;

	queue_listen(LISTEN_UNLISTEN, channel);
}

/*
 * Async_UnlistenAll
 *
 *		This is invoked by UNLISTEN * command, and also at backend exit.
 */
void
Async_UnlistenAll(void)
{
	if (Trace_notify)
		elog(DEBUG1, "Async_UnlistenAll(%d)", MyProcPid);

	/* If we couldn't possibly be listening, no need to queue anything */
	if (pendingActions == NULL && !unlistenExitRegistered)
		return;

	queue_listen(LISTEN_UNLISTEN_ALL, "");
}

/*
 * SQL function: return a set of the channel names this backend is actively
 * listening to.
 *
 * Note: this coding relies on the fact that the listenChannels list cannot
 * change within a transaction.
 */
Datum
pg_listening_channels(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < list_length(listenChannels))
	{
		char	   *channel = (char *) list_nth(listenChannels,
												funcctx->call_cntr);

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(channel));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Async_UnlistenOnExit
 *
 * This is executed at backend exit if we have done any LISTENs in this
 * backend.  It might not be necessary anymore, if the user UNLISTENed
 * everything, but we don't try to detect that case.
 */
static void
Async_UnlistenOnExit(int code, Datum arg)
{
	Exec_UnlistenAllCommit();
	asyncQueueUnregister();
}

/*
 * AtPrepare_Notify
 *
 *		This is called at the prepare phase of a two-phase
 *		transaction.  Save the state for possible commit later.
 */
void
AtPrepare_Notify(void)
{
	/* It's not allowed to have any pending LISTEN/UNLISTEN/NOTIFY actions */
	if (pendingActions || pendingNotifies)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has executed LISTEN, UNLISTEN, or NOTIFY")));
}

/*
 * PreCommit_Notify
 *
 *		This is called at transaction commit, before actually committing to
 *		clog.
 *
 *		If there are pending LISTEN actions, make sure we are listed in the
 *		shared-memory listener array.  This must happen before commit to
 *		ensure we don't miss any notifies from transactions that commit
 *		just after ours.
 *
 *		If there are outbound notify requests in the pendingNotifies list,
 *		add them to the global queue.  We do that before commit so that
 *		we can still throw error if we run out of queue space.
 */
void
PreCommit_Notify(void)
{
	ListCell   *p;

	if (!pendingActions && !pendingNotifies)
		return;					/* no relevant statements in this xact */

	if (Trace_notify)
		elog(DEBUG1, "PreCommit_Notify");

	/* Preflight for any pending listen/unlisten actions */
	if (pendingActions != NULL)
	{
		foreach(p, pendingActions->actions)
		{
			ListenAction *actrec = (ListenAction *) lfirst(p);

			switch (actrec->action)
			{
				case LISTEN_LISTEN:
					Exec_ListenPreCommit();
					break;
				case LISTEN_UNLISTEN:
					/* there is no Exec_UnlistenPreCommit() */
					break;
				case LISTEN_UNLISTEN_ALL:
					/* there is no Exec_UnlistenAllPreCommit() */
					break;
			}
		}
	}

	/* Write notification data to WAL if we have any */
	if (pendingNotifies)
	{
		TransactionId currentXid;
		ListCell   *l;
		size_t		total_size = 0;
		uint32		nnotifications = 0;
		char	   *notifications_data;
		char	   *ptr;
		XLogRecPtr	notify_lsn;

		/*
		 * Make sure that we have an XID assigned to the current transaction.
		 * GetCurrentTransactionId is cheap if we already have an XID, but not
		 * so cheap if we don't.
		 */
		currentXid = GetCurrentTransactionId();

		/*
		 * Step 1: Reserve space in the in-memory queue for the compact entry.
		 */
		LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
		{
			QueuePosition reserved_head = QUEUE_HEAD + asyncQueueControl->reservedEntries;
			int64 headPage = QUEUE_POS_PAGE(reserved_head);
			int   headSlot = (int) (reserved_head % ASYNC_ENTRIES_PER_PAGE);
			int64 tailPage = QUEUE_POS_PAGE(QUEUE_TAIL);
			/* Also cap total entries irrespective of page math */
			int64 max_total_entries = (int64) max_notify_queue_pages * ASYNC_ENTRIES_PER_PAGE;
			int64 current_total_entries = (reserved_head - QUEUE_TAIL) + 1; /* +1 for our reservation */
			LWLock *nextbank;

			/* Check page-window fullness first */
			if (asyncQueuePageDiff(headPage, tailPage) >= max_notify_queue_pages)
			{
				LWLockRelease(NotifyQueueLock);
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("could not queue notification before commit"),
						 errdetail("asynchronous notification queue is full")));
			}

			/* If at last slot, ensure advancing to next page is allowed */
			if (headSlot == ASYNC_ENTRIES_PER_PAGE - 1)
			{
				if (asyncQueuePageDiff(headPage + 1, tailPage) >= max_notify_queue_pages)
				{
					LWLockRelease(NotifyQueueLock);
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
							 errmsg("could not queue notification before commit"),
							 errdetail("asynchronous notification queue is full")));
				}

				/* Pre-initialize the next page so commit path doesn't fault it in */
				nextbank = SimpleLruGetBankLock(NotifyCtl, headPage + 1);
				LWLockAcquire(nextbank, LW_EXCLUSIVE);
				(void) SimpleLruZeroPage(NotifyCtl, headPage + 1);
				LWLockRelease(nextbank);
			}

			/* Also enforce a strict entry-count limit */
			if (current_total_entries > max_total_entries)
			{
				LWLockRelease(NotifyQueueLock);
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("could not queue notification before commit"),
						 errdetail("asynchronous notification queue is full")));
			}

			/* Reserve one entry */
			asyncQueueControl->reservedEntries++;
			notifyEntryReserved = true;
		}
		LWLockRelease(NotifyQueueLock);

		/*
		 * Step 2: Write notification data to WAL.
		 */
		/* First pass: calculate total size needed for serialization */
		foreach(l, pendingNotifies->events)
		{
			Notification *n = (Notification *) lfirst(l);

			/* Size: 2 bytes for channel_len + 2 bytes for payload_len + strings */
			total_size += 4 + n->channel_len + 1 + n->payload_len + 1;
			nnotifications++;
		}

		/* Allocate buffer for notification data */
		notifications_data = palloc(total_size);
		ptr = notifications_data;

		/* Second pass: serialize all notifications */
		foreach(l, pendingNotifies->events)
		{
			Notification *n = (Notification *) lfirst(l);
			char	   *channel = n->data;
			char	   *payload = n->data + n->channel_len + 1;

			/* Write channel length, payload length, channel, and payload */
			memcpy(ptr, &n->channel_len, 2);
			ptr += 2;
			memcpy(ptr, &n->payload_len, 2);
			ptr += 2;
			memcpy(ptr, channel, n->channel_len + 1);
			ptr += n->channel_len + 1;
			memcpy(ptr, payload, n->payload_len + 1);
			ptr += n->payload_len + 1;
		}

		/*
		 * To avoid a race with WAL recycling, hold NotifyQueueLock EXCLUSIVE
		 * across the WAL insert and publication of the uncommitted pin.
		 */
		LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);

		/* Write notification data to WAL */
		notify_lsn = LogAsyncNotifyData(MyDatabaseId, currentXid, MyProcPid,
										nnotifications, total_size,
										notifications_data);

		pfree(notifications_data);

        MyProc->notifyCommitLsn = notify_lsn;
		UncommittedNotifyAdd(GetTopFullTransactionId(), notify_lsn);

		LWLockRelease(NotifyQueueLock);

		/* Notification payloads are now read directly from WAL at delivery time. */
	}
}

/*
 * AtCommit_Notify
 *
 *		This is called at transaction commit, after committing to clog.
 *
 *		Update listenChannels and clear transaction-local state.
 *
 *		If we issued any notifications in the transaction, send signals to
 *		listening backends (possibly including ourselves) to process them.
 *		Also, if we filled enough queue pages with new notifies, try to
 *		advance the queue tail pointer.
 */
void
AtCommit_Notify(void)
{
	ListCell   *p;

	/*
	 * Allow transactions that have not executed LISTEN/UNLISTEN/NOTIFY to
	 * return as soon as possible
	 */
	if (!pendingActions && !pendingNotifies)
		return;

	if (Trace_notify)
		elog(DEBUG1, "AtCommit_Notify");

	/* Perform any pending listen/unlisten actions */
	if (pendingActions != NULL)
	{
		foreach(p, pendingActions->actions)
		{
			ListenAction *actrec = (ListenAction *) lfirst(p);

			switch (actrec->action)
			{
				case LISTEN_LISTEN:
					Exec_ListenCommit(actrec->channel);
					break;
				case LISTEN_UNLISTEN:
					Exec_UnlistenCommit(actrec->channel);
					break;
				case LISTEN_UNLISTEN_ALL:
					Exec_UnlistenAllCommit();
					break;
			}
		}
	}

	/* If no longer listening to anything, get out of listener array */
	if (amRegisteredListener && listenChannels == NIL)
		asyncQueueUnregister();

	/*
	 * If we had notifications, they were already written to the queue in
	 * PreCommit_Notify. After commit, signal listening backends to check the
	 * queue. The transaction visibility logic will see our
	 * XID as committed and process the notifications.
	 */
	if (!XLogRecPtrIsInvalid(MyProc->notifyCommitLsn))
	{
		/* Signal listening backends to check the queue */
		SignalBackends();

		/* Clear the flag after signaling */
		MyProc->notifyCommitLsn = InvalidXLogRecPtr;
	}

	/*
	 * If it's time to try to advance the global tail pointer, do that.
	 *
	 * (It might seem odd to do this in the sender, when more than likely the
	 * listeners won't yet have read the messages we just sent.  However,
	 * there's less contention if only the sender does it, and there is little
	 * need for urgency in advancing the global tail.  So this typically will
	 * be clearing out messages that were sent some time ago.)
	 */
	if (tryAdvanceTail)
	{
		tryAdvanceTail = false;
		asyncQueueAdvanceTail();
	}

	/* And clean up */
	ClearPendingActionsAndNotifies();

	/* Reset local reservation flag if set (reservation consumed at commit). */
	notifyEntryReserved = false;
}

/*
 * Exec_ListenPreCommit --- subroutine for PreCommit_Notify
 *
 * This function must make sure we are ready to catch any incoming messages.
 */
static void
Exec_ListenPreCommit(void)
{
	QueuePosition head;
	QueuePosition max;
	ProcNumber	prevListener;

	/*
	 * Nothing to do if we are already listening to something, nor if we
	 * already ran this routine in this transaction.
	 */
	if (amRegisteredListener)
		return;

	if (Trace_notify)
		elog(DEBUG1, "Exec_ListenPreCommit(%d)", MyProcPid);

	/*
	 * Before registering, make sure we will unlisten before dying. (Note:
	 * this action does not get undone if we abort later.)
	 */
	if (!unlistenExitRegistered)
	{
		before_shmem_exit(Async_UnlistenOnExit, 0);
		unlistenExitRegistered = true;
	}

	/*
	 * This is our first LISTEN, so establish our pointer.
	 *
	 * We set our pointer to the global tail pointer and then move it forward
	 * over already-committed notifications.  This ensures we cannot miss any
	 * not-yet-committed notifications.  We might get a few more but that
	 * doesn't hurt.
	 *
	 * In some scenarios there might be a lot of committed notifications that
	 * have not yet been pruned away (because some backend is being lazy about
	 * reading them).  To reduce our startup time, we can look at other
	 * backends and adopt the maximum "pos" pointer of any backend that's in
	 * our database; any notifications it's already advanced over are surely
	 * committed and need not be re-examined by us.  (We must consider only
	 * backends connected to our DB, because others will not have bothered to
	 * check committed-ness of notifications in our DB.)
	 *
	 * We need exclusive lock here so we can look at other backends' entries
	 * and manipulate the list links.
	 */
	LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
	head = QUEUE_HEAD;
	max = QUEUE_TAIL;
	prevListener = INVALID_PROC_NUMBER;
	for (ProcNumber i = QUEUE_FIRST_LISTENER; i != INVALID_PROC_NUMBER; i = QUEUE_NEXT_LISTENER(i))
	{
		if (QUEUE_BACKEND_DBOID(i) == MyDatabaseId)
			max = QUEUE_POS_MAX(max, QUEUE_BACKEND_POS(i));
		/* Also find last listening backend before this one */
		if (i < MyProcNumber)
			prevListener = i;
	}
	QUEUE_BACKEND_POS(MyProcNumber) = max;
	QUEUE_BACKEND_PID(MyProcNumber) = MyProcPid;
	QUEUE_BACKEND_DBOID(MyProcNumber) = MyDatabaseId;
	/* Insert backend into list of listeners at correct position */
	if (prevListener != INVALID_PROC_NUMBER)
	{
		QUEUE_NEXT_LISTENER(MyProcNumber) = QUEUE_NEXT_LISTENER(prevListener);
		QUEUE_NEXT_LISTENER(prevListener) = MyProcNumber;
	}
	else
	{
		QUEUE_NEXT_LISTENER(MyProcNumber) = QUEUE_FIRST_LISTENER;
		QUEUE_FIRST_LISTENER = MyProcNumber;
	}
	LWLockRelease(NotifyQueueLock);

	/* Now we are listed in the global array, so remember we're listening */
	amRegisteredListener = true;

	/*
	 * Try to move our pointer forward as far as possible.  This will skip
	 * over already-committed notifications, which we want to do because they
	 * might be quite stale.  Note that we are not yet listening on anything,
	 * so we won't deliver such notifications to our frontend.  Also, although
	 * our transaction might have executed NOTIFY, those message(s) aren't
	 * queued yet so we won't skip them here.
	 */
	if (!QUEUE_POS_EQUAL(max, head))
		asyncQueueReadAllNotifications();
}

/*
 * Exec_ListenCommit --- subroutine for AtCommit_Notify
 *
 * Add the channel to the list of channels we are listening on.
 */
static void
Exec_ListenCommit(const char *channel)
{
	MemoryContext oldcontext;

	/* Do nothing if we are already listening on this channel */
	if (IsListeningOn(channel))
		return;

	/*
	 * Add the new channel name to listenChannels.
	 *
	 * XXX It is theoretically possible to get an out-of-memory failure here,
	 * which would be bad because we already committed.  For the moment it
	 * doesn't seem worth trying to guard against that, but maybe improve this
	 * later.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	listenChannels = lappend(listenChannels, pstrdup(channel));
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Exec_UnlistenCommit --- subroutine for AtCommit_Notify
 *
 * Remove the specified channel name from listenChannels.
 */
static void
Exec_UnlistenCommit(const char *channel)
{
	ListCell   *q;

	if (Trace_notify)
		elog(DEBUG1, "Exec_UnlistenCommit(%s,%d)", channel, MyProcPid);

	foreach(q, listenChannels)
	{
		char	   *lchan = (char *) lfirst(q);

		if (strcmp(lchan, channel) == 0)
		{
			listenChannels = foreach_delete_current(listenChannels, q);
			pfree(lchan);
			break;
		}
	}

	/*
	 * We do not complain about unlistening something not being listened;
	 * should we?
	 */
}

/*
 * Exec_UnlistenAllCommit --- subroutine for AtCommit_Notify
 *
 *		Unlisten on all channels for this backend.
 */
static void
Exec_UnlistenAllCommit(void)
{
	if (Trace_notify)
		elog(DEBUG1, "Exec_UnlistenAllCommit(%d)", MyProcPid);

	list_free_deep(listenChannels);
	listenChannels = NIL;
}

/*
 * Test whether we are actively listening on the given channel name.
 *
 * Note: this function is executed for every notification found in the queue.
 * Perhaps it is worth further optimization, eg convert the list to a sorted
 * array so we can binary-search it.  In practice the list is likely to be
 * fairly short, though.
 */
static bool
IsListeningOn(const char *channel)
{
	ListCell   *p;

	foreach(p, listenChannels)
	{
		char	   *lchan = (char *) lfirst(p);

		if (strcmp(lchan, channel) == 0)
			return true;
	}
	return false;
}

/*
 * Remove our entry from the listeners array when we are no longer listening
 * on any channel.  NB: must not fail if we're already not listening.
 */
static void
asyncQueueUnregister(void)
{
	Assert(listenChannels == NIL);	/* else caller error */

	if (!amRegisteredListener)	/* nothing to do */
		return;

	/*
	 * Need exclusive lock here to manipulate list links.
	 */
	LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
	/* Mark our entry as invalid */
	QUEUE_BACKEND_PID(MyProcNumber) = InvalidPid;
	QUEUE_BACKEND_DBOID(MyProcNumber) = InvalidOid;
	/* and remove it from the list */
	if (QUEUE_FIRST_LISTENER == MyProcNumber)
		QUEUE_FIRST_LISTENER = QUEUE_NEXT_LISTENER(MyProcNumber);
	else
	{
		for (ProcNumber i = QUEUE_FIRST_LISTENER; i != INVALID_PROC_NUMBER; i = QUEUE_NEXT_LISTENER(i))
		{
			if (QUEUE_NEXT_LISTENER(i) == MyProcNumber)
			{
				QUEUE_NEXT_LISTENER(i) = QUEUE_NEXT_LISTENER(MyProcNumber);
				break;
			}
		}
	}
	QUEUE_NEXT_LISTENER(MyProcNumber) = INVALID_PROC_NUMBER;
	LWLockRelease(NotifyQueueLock);

	/* mark ourselves as no longer listed in the global array */
	amRegisteredListener = false;
}

/*
 * Advance the QueuePosition to the next entry, assuming that the current
 * entry is of length entryLength.  If we jump to a new page the function
 * returns true, else false.
 */
static bool
asyncQueueAdvance(volatile QueuePosition *position, int entryLength)
{
	int64 idx;
	bool pageJump;

	/* With fixed-size entries, advancing is just +1 entry. */
	Assert(entryLength == (int) sizeof(AsyncQueueEntry));
	idx = *position;
	pageJump = ((idx % ASYNC_ENTRIES_PER_PAGE) == (ASYNC_ENTRIES_PER_PAGE - 1));
	*position = idx + 1;
	return pageJump;
}

/*
 * SQL function to return the fraction of the notification queue currently
 * occupied.
 */
Datum
pg_notification_queue_usage(PG_FUNCTION_ARGS)
{
	double		usage;

	/* Advance the queue tail so we don't report a too-large result */
	asyncQueueAdvanceTail();

	LWLockAcquire(NotifyQueueLock, LW_SHARED);
	usage = asyncQueueUsage();
	LWLockRelease(NotifyQueueLock);

	PG_RETURN_FLOAT8(usage);
}

/*
 * Return the fraction of the queue that is currently occupied.
 *
 * The caller must hold NotifyQueueLock in (at least) shared mode.
 *
 * Note: we measure the distance to the logical tail page, not the physical
 * tail page.  In some sense that's wrong, but the relative position of the
 * physical tail is affected by details such as SLRU segment boundaries,
 * so that a result based on that is unpleasantly unstable.
 */
static double
asyncQueueUsage(void)
{
	int64		headPage = QUEUE_POS_PAGE(QUEUE_HEAD);
	int64		tailPage = QUEUE_POS_PAGE(QUEUE_TAIL);
	int64		occupied = headPage - tailPage;

	if (occupied == 0)
		return (double) 0;		/* fast exit for common case */

	return (double) occupied / (double) max_notify_queue_pages;
}


/*
 * Send signals to listening backends.
 *
 * Normally we signal only backends in our own database, since only those
 * backends could be interested in notifies we send.  However, if there's
 * notify traffic in our database but no traffic in another database that
 * does have listener(s), those listeners will fall further and further
 * behind.  Waken them anyway if they're far enough behind, so that they'll
 * advance their queue position pointers, allowing the global tail to advance.
 *
 * Since we know the ProcNumber and the Pid the signaling is quite cheap.
 *
 * This is called during CommitTransaction(), so it's important for it
 * to have very low probability of failure.
 */
static void
SignalBackends(void)
{
	int32	   *pids;
	ProcNumber *procnos;
	int			count;

	/*
	 * Identify backends that we need to signal.  We don't want to send
	 * signals while holding the NotifyQueueLock, so this loop just builds a
	 * list of target PIDs.
	 *
	 * XXX in principle these pallocs could fail, which would be bad. Maybe
	 * preallocate the arrays?  They're not that large, though.
	 */
	pids = (int32 *) palloc(MaxBackends * sizeof(int32));
	procnos = (ProcNumber *) palloc(MaxBackends * sizeof(ProcNumber));
	count = 0;

	LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
	for (ProcNumber i = QUEUE_FIRST_LISTENER; i != INVALID_PROC_NUMBER; i = QUEUE_NEXT_LISTENER(i))
	{
		int32		pid = QUEUE_BACKEND_PID(i);
		QueuePosition pos;

		Assert(pid != InvalidPid);
		pos = QUEUE_BACKEND_POS(i);
		if (QUEUE_BACKEND_DBOID(i) == MyDatabaseId)
		{
			/*
			 * Always signal listeners in our own database, unless they're
			 * already caught up (unlikely, but possible).
			 */
			if (QUEUE_POS_EQUAL(pos, QUEUE_HEAD))
				continue;
		}
		else
		{
			/*
			 * Listeners in other databases should be signaled only if they
			 * are far behind.
			 */
			if (asyncQueuePageDiff(QUEUE_POS_PAGE(QUEUE_HEAD),
								   QUEUE_POS_PAGE(pos)) < QUEUE_CLEANUP_DELAY)
				continue;
		}
		/* OK, need to signal this one */
		pids[count] = pid;
		procnos[count] = i;
		count++;
	}
	LWLockRelease(NotifyQueueLock);

	/* Now send signals */
	for (int i = 0; i < count; i++)
	{
		int32		pid = pids[i];

		/*
		 * If we are signaling our own process, no need to involve the kernel;
		 * just set the flag directly.
		 */
		if (pid == MyProcPid)
		{
			notifyInterruptPending = true;
			continue;
		}

		/*
		 * Note: assuming things aren't broken, a signal failure here could
		 * only occur if the target backend exited since we released
		 * NotifyQueueLock; which is unlikely but certainly possible. So we
		 * just log a low-level debug message if it happens.
		 */
		if (SendProcSignal(pid, PROCSIG_NOTIFY_INTERRUPT, procnos[i]) < 0)
			elog(DEBUG3, "could not signal backend with PID %d: %m", pid);
	}

	pfree(pids);
	pfree(procnos);
}

/*
 * AtAbort_Notify
 *
 *	This is called at transaction abort.
 *
 *	Gets rid of pending actions and outbound notifies that we would have
 *	executed if the transaction got committed.
 */
void
AtAbort_Notify(void)
{
	/*
	 * If we LISTEN but then roll back the transaction after PreCommit_Notify,
	 * we have registered as a listener but have not made any entry in
	 * listenChannels.  In that case, deregister again.
	 */
	if (amRegisteredListener && listenChannels == NIL)
		asyncQueueUnregister();

	/* Release any reserved queue entry */
	if (notifyEntryReserved)
	{
		LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
		if (asyncQueueControl->reservedEntries > 0)
			asyncQueueControl->reservedEntries--;
		LWLockRelease(NotifyQueueLock);
		notifyEntryReserved = false;
	}

	/* And clean up */
	ClearPendingActionsAndNotifies();
}

/*
 * AtSubCommit_Notify() --- Take care of subtransaction commit.
 *
 * Reassign all items in the pending lists to the parent transaction.
 */
void
AtSubCommit_Notify(void)
{
	int			my_level = GetCurrentTransactionNestLevel();

	/* If there are actions at our nesting level, we must reparent them. */
	if (pendingActions != NULL &&
		pendingActions->nestingLevel >= my_level)
	{
		if (pendingActions->upper == NULL ||
			pendingActions->upper->nestingLevel < my_level - 1)
		{
			/* nothing to merge; give the whole thing to the parent */
			--pendingActions->nestingLevel;
		}
		else
		{
			ActionList *childPendingActions = pendingActions;

			pendingActions = pendingActions->upper;

			/*
			 * Mustn't try to eliminate duplicates here --- see queue_listen()
			 */
			pendingActions->actions =
				list_concat(pendingActions->actions,
							childPendingActions->actions);
			pfree(childPendingActions);
		}
	}

	/* If there are notifies at our nesting level, we must reparent them. */
	if (pendingNotifies != NULL &&
		pendingNotifies->nestingLevel >= my_level)
	{
		Assert(pendingNotifies->nestingLevel == my_level);

		if (pendingNotifies->upper == NULL ||
			pendingNotifies->upper->nestingLevel < my_level - 1)
		{
			/* nothing to merge; give the whole thing to the parent */
			--pendingNotifies->nestingLevel;
		}
		else
		{
			/*
			 * Formerly, we didn't bother to eliminate duplicates here, but
			 * now we must, else we fall foul of "Assert(!found)", either here
			 * or during a later attempt to build the parent-level hashtable.
			 */
			NotificationList *childPendingNotifies = pendingNotifies;
			ListCell   *l;

			pendingNotifies = pendingNotifies->upper;
			/* Insert all the subxact's events into parent, except for dups */
			foreach(l, childPendingNotifies->events)
			{
				Notification *childn = (Notification *) lfirst(l);

				if (!AsyncExistsPendingNotify(childn))
					AddEventToPendingNotifies(childn);
			}
			pfree(childPendingNotifies);
		}
	}
}

/*
 * AtSubAbort_Notify() --- Take care of subtransaction abort.
 */
void
AtSubAbort_Notify(void)
{
	int			my_level = GetCurrentTransactionNestLevel();

	/*
	 * All we have to do is pop the stack --- the actions/notifies made in
	 * this subxact are no longer interesting, and the space will be freed
	 * when CurTransactionContext is recycled. We still have to free the
	 * ActionList and NotificationList objects themselves, though, because
	 * those are allocated in TopTransactionContext.
	 *
	 * Note that there might be no entries at all, or no entries for the
	 * current subtransaction level, either because none were ever created, or
	 * because we reentered this routine due to trouble during subxact abort.
	 */
	while (pendingActions != NULL &&
		   pendingActions->nestingLevel >= my_level)
	{
		ActionList *childPendingActions = pendingActions;

		pendingActions = pendingActions->upper;
		pfree(childPendingActions);
	}

	while (pendingNotifies != NULL &&
		   pendingNotifies->nestingLevel >= my_level)
	{
		NotificationList *childPendingNotifies = pendingNotifies;

		pendingNotifies = pendingNotifies->upper;
		pfree(childPendingNotifies);
	}
}

/*
 * HandleNotifyInterrupt
 *
 *		Signal handler portion of interrupt handling. Let the backend know
 *		that there's a pending notify interrupt. If we're currently reading
 *		from the client, this will interrupt the read and
 *		ProcessClientReadInterrupt() will call ProcessNotifyInterrupt().
 */
void
HandleNotifyInterrupt(void)
{
	/*
	 * Note: this is called by a SIGNAL HANDLER. You must be very wary what
	 * you do here.
	 */

	/* signal that work needs to be done */
	notifyInterruptPending = true;

	/* make sure the event is processed in due course */
	SetLatch(MyLatch);
}

/*
 * ProcessNotifyInterrupt
 *
 *		This is called if we see notifyInterruptPending set, just before
 *		transmitting ReadyForQuery at the end of a frontend command, and
 *		also if a notify signal occurs while reading from the frontend.
 *		HandleNotifyInterrupt() will cause the read to be interrupted
 *		via the process's latch, and this routine will get called.
 *		If we are truly idle (ie, *not* inside a transaction block),
 *		process the incoming notifies.
 *
 *		If "flush" is true, force any frontend messages out immediately.
 *		This can be false when being called at the end of a frontend command,
 *		since we'll flush after sending ReadyForQuery.
 */
void
ProcessNotifyInterrupt(bool flush)
{
	if (IsTransactionOrTransactionBlock())
		return;					/* not really idle */

	/* Loop in case another signal arrives while sending messages */
	while (notifyInterruptPending)
		ProcessIncomingNotify(flush);
}


/*
 * Read all pending notifications from the queue, and deliver appropriate
 * ones to my frontend.  Stop when we reach queue head.
 */
static void
asyncQueueReadAllNotifications(void)
{
	volatile QueuePosition pos;
	QueuePosition head;

	/* page_buffer must be adequately aligned, so use a union */
	union
	{
		char		buf[QUEUE_PAGESIZE];
		AsyncQueueEntry align;
	}			page_buffer;

	/* Fetch current state */
	LWLockAcquire(NotifyQueueLock, LW_SHARED);
	/* Assert checks that we have a valid state entry */
	Assert(MyProcPid == QUEUE_BACKEND_PID(MyProcNumber));
	pos = QUEUE_BACKEND_POS(MyProcNumber);
	head = QUEUE_HEAD;
	LWLockRelease(NotifyQueueLock);

	if (QUEUE_POS_EQUAL(pos, head))
	{
		/* Nothing to do, we have read all notifications already. */
		return;
	}


	/*
	 * It is possible that we fail while trying to send a message to our
	 * frontend (for example, because of encoding conversion failure).  If
	 * that happens it is critical that we not try to send the same message
	 * over and over again.  Therefore, we place a PG_TRY block here that will
	 * forcibly advance our queue position before we lose control to an error.
	 * (We could alternatively retake NotifyQueueLock and move the position
	 * before handling each individual message, but that seems like too much
	 * lock traffic.)
	 */
	PG_TRY();
	{
		bool		reachedStop;

		do
		{
			int64		curpage = QUEUE_POS_PAGE(pos);
			int			curoffset = QUEUE_POS_OFFSET(pos);
			int			slotno;
			int			copysize;

			/*
			 * We copy the data from SLRU into a local buffer, so as to avoid
			 * holding the SLRU lock while we are examining the entries and
			 * possibly transmitting them to our frontend.  Copy only the part
			 * of the page we will actually inspect.
			 */
			slotno = SimpleLruReadPage_ReadOnly(NotifyCtl, curpage,
												InvalidTransactionId);
			if (curpage == QUEUE_POS_PAGE(head))
			{
				/* we only want to read as far as head */
				copysize = QUEUE_POS_OFFSET(head) - curoffset;
				if (copysize < 0)
					copysize = 0;	/* just for safety */
			}
			else
			{
				/* fetch all the rest of the page */
				copysize = QUEUE_PAGESIZE - curoffset;
			}
			memcpy(page_buffer.buf + curoffset,
				   NotifyCtl->shared->page_buffer[slotno] + curoffset,
				   copysize);
			/* Release lock that we got from SimpleLruReadPage_ReadOnly() */
			LWLockRelease(SimpleLruGetBankLock(NotifyCtl, curpage));

			/*
			 * Process messages up to the stop position, end of page, or an
			 * uncommitted message.
			 *
			 * Our stop position is what we found to be the head's position
			 * when we entered this function. It might have changed already.
			 * But if it has, we will receive (or have already received and
			 * queued) another signal and come here again.
			 *
			 * We are not holding NotifyQueueLock here! The queue can only
			 * extend beyond the head pointer (see above) and we leave our
			 * backend's pointer where it is so nobody will truncate or
			 * rewrite pages under us. Especially we don't want to hold a lock
			 * while sending the notifications to the frontend.
			 */
			reachedStop = asyncQueueProcessPageEntries(&pos, head,
													   page_buffer.buf);
		} while (!reachedStop);
	}
	PG_FINALLY();
	{
		/* Update shared state */
		LWLockAcquire(NotifyQueueLock, LW_SHARED);
		QUEUE_BACKEND_POS(MyProcNumber) = pos;
		LWLockRelease(NotifyQueueLock);
	}
	PG_END_TRY();

}

/*
 * Fetch notifications from the shared queue, beginning at position current,
 * and deliver relevant ones to my frontend.
 *
 * The current page must have been fetched into page_buffer from shared
 * memory.  (We could access the page right in shared memory, but that
 * would imply holding the SLRU bank lock throughout this routine.)
 *
 * We stop if we reach the "stop" position or reach the end of the page.
 *
 * The function returns true once we have reached the stop position, and false
 * if we have finished with the page.
 * In other words: once it returns true there is no need to look further.
 * The QueuePosition *current is advanced past all processed messages.
 */
static bool
asyncQueueProcessPageEntries(volatile QueuePosition *current,
							 QueuePosition stop,
							 char *page_buffer)
{
	bool		reachedStop = false;
	bool		reachedEndOfPage;
	AsyncQueueEntry *qe;

	do
	{
		QueuePosition thisentry = *current;

		if (QUEUE_POS_EQUAL(thisentry, stop))
			break;

		qe = (AsyncQueueEntry *) (page_buffer + QUEUE_POS_OFFSET(thisentry));

		/* Advance *current by one fixed-size compact entry. */
		reachedEndOfPage = asyncQueueAdvance(current, sizeof(AsyncQueueEntry));

		/* Ignore messages destined for other databases */
		if (qe->dboid == MyDatabaseId)
		{
			/*
			 * Since queue entries are written atomically with commit records
			 * while holding NotifyQueueLock exclusively, all entries in the queue
			 * are guaranteed to be from committed transactions.
			 *
			 * Step 5: Read notification data using stored LSN from WAL.
			 * The compact entry only contains metadata.
			 */
			processNotificationFromWAL(qe->notify_lsn);
		}

		/* Loop back if we're not at end of page */
	} while (!reachedEndOfPage);

	if (QUEUE_POS_EQUAL(*current, stop))
		reachedStop = true;

	return reachedStop;
}

/*
 * processNotificationFromWAL
 *
 * Fetch notification data from WAL using the stored LSN and process
 * the individual notifications for delivery to listening frontend.
 * This implements Step 5 of the new WAL-based notification system.
 */
static void
processNotificationFromWAL(XLogRecPtr notify_lsn)
{
	XLogReaderState *xlogreader;
	DecodedXLogRecord *record;
	xl_async_notify_data *xlrec;
	char	   *data;
	char	   *ptr;
	uint32_t	remaining;
	int			srcPid;
	char	   *errormsg;
	Oid			dboid;
	uint32		nnotifications;

	/*
	 * Create XLog reader to fetch the notification data record.
	 * We use a temporary reader since this is called during normal
	 * notification processing, not during recovery.
	 */
	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									NULL);
	if (!xlogreader)
		elog(ERROR, "failed to allocate XLog reader for notification data");

	/* Start reading exactly at the NOTIFY_DATA record begin LSN */
	XLogBeginRead(xlogreader, notify_lsn);

	/* Read the NOTIFY_DATA record */
	record = (DecodedXLogRecord *) XLogReadRecord(xlogreader, &errormsg);
	if (record == NULL)
	{
		XLogReaderFree(xlogreader);
		elog(ERROR, "failed to read notification data from WAL at %X/%X: %s",
			 LSN_FORMAT_ARGS(notify_lsn), errormsg ? errormsg : "no error message");
	}

	/* Verify this is the expected record type */
	if (XLogRecGetRmid(xlogreader) != RM_ASYNC_ID ||
		(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_ASYNC_NOTIFY_DATA)
		elog(ERROR, "expected NOTIFY_DATA at %X/%X, found rmgr %u info %u",
			 LSN_FORMAT_ARGS(notify_lsn),
			 XLogRecGetRmid(xlogreader),
			 (XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK));

	/* Extract the notification data from the WAL record */
	xlrec = (xl_async_notify_data *) XLogRecGetData(xlogreader);
	srcPid = xlrec->srcPid;
	dboid = xlrec->dbid;
	data = (char *) xlrec + SizeOfAsyncNotifyData;
	ptr = data;
	remaining = XLogRecGetDataLen(xlogreader) - SizeOfAsyncNotifyData;
	nnotifications = xlrec->nnotifications;

	/*
	 * Process each notification in the serialized data.
	 * The format is: 2-byte channel_len, 2-byte payload_len,
	 * null-terminated channel, null-terminated payload.
	 */
	for (uint32_t i = 0; i < nnotifications && remaining >= 4; i++)
	{
		uint16		channel_len;
		uint16		payload_len;
		char	   *channel;
		char	   *payload;

		/* Read lengths */
		memcpy(&channel_len, ptr, 2);
		ptr += 2;
		memcpy(&payload_len, ptr, 2);
		ptr += 2;
		remaining -= 4;

		/* Verify we have enough data */
		if (remaining < channel_len + 1 + payload_len + 1)
			break;

		/* Extract channel and payload strings */
		channel = ptr;
		ptr += channel_len + 1;
		payload = ptr;
		ptr += payload_len + 1;
		remaining -= (channel_len + 1 + payload_len + 1);

		/* Deliver notification if we're listening on this channel */
		if (dboid == MyDatabaseId && IsListeningOn(channel))
			NotifyMyFrontEnd(channel, payload, srcPid);
	}

	/* Clean up */
	XLogReaderFree(xlogreader);
}


/*
 * AsyncNotifyOldestRequiredLSN
 *
 * Compute the oldest WAL LSN required to satisfy NOTIFY delivery for any
 * still-present queue entry. Returns true and sets *oldest_lsn when the
 * queue is non-empty (QUEUE_TAIL != QUEUE_HEAD). Otherwise returns false.
 *
 * We look at the queue entry at QUEUE_TAIL; since that is the oldest entry
 * still needed by some listener, its notify_lsn is the minimum WAL position
 * that must be retained.
 */
bool
AsyncNotifyOldestRequiredLSN(XLogRecPtr *oldest_lsn)
{
	XLogRecPtr committed_min = InvalidXLogRecPtr;
	XLogRecPtr uncommitted_min = InvalidXLogRecPtr;
	bool have_any = false;
	int i;
	TransactionId xid;

	/*
	 * Hold a single EXCLUSIVE lock while reading both structures to avoid
	 * races with the commit path that updates page mins and removes
	 * uncommitted entries atomically under the same lock.
	 */
	LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);

	/* Committed side: scan per-page mins */
	if (NotifyPageMins != NULL)
	{
		for (i = 0; i < max_notify_queue_pages; i++)
		{
			if (NotifyPageMins[i].page_no >= 0 && !XLogRecPtrIsInvalid(NotifyPageMins[i].min_lsn))
			{
				if (XLogRecPtrIsInvalid(committed_min) || (NotifyPageMins[i].min_lsn < committed_min))
					committed_min = NotifyPageMins[i].min_lsn;
			}
		}
	}

	/* Uncommitted side: prune stale and compute min */
	if (UncommittedNotifies != NULL)
	{
		for (i = 0; i < MaxBackends; i++)
		{
			if (!UncommittedNotifies[i].in_use)
				continue;
			xid = XidFromFullTransactionId(UncommittedNotifies[i].fxid);
			if (!TransactionIdIsInProgress(xid))
			{
				/* Stale entry, drop it */
				UncommittedNotifies[i].in_use = false;
				UncommittedNotifies[i].fxid = InvalidFullTransactionId;
				UncommittedNotifies[i].write_lsn = InvalidXLogRecPtr;
				continue;
			}
			if (XLogRecPtrIsInvalid(uncommitted_min) || (UncommittedNotifies[i].write_lsn < uncommitted_min))
				uncommitted_min = UncommittedNotifies[i].write_lsn;
		}
	}

	/* Choose the minimum among committed and uncommitted (ignoring Invalid) */
	if (!XLogRecPtrIsInvalid(committed_min))
	{
		*oldest_lsn = committed_min;
		have_any = true;
	}
	if (!XLogRecPtrIsInvalid(uncommitted_min))
	{
		if (!have_any || (uncommitted_min < *oldest_lsn))
			*oldest_lsn = uncommitted_min;
		have_any = true;
	}

	LWLockRelease(NotifyQueueLock);
	return have_any;
}


/*
 * asyncQueueAddCompactEntry
 *
 * Add a compact entry to the notification SLRU queue containing only
 * metadata (dbid, xid, notify_lsn) that points to the full notification 
 * data in WAL. This is much more efficient than the old approach of
 * storing complete notification content in the SLRU queue.
 */
void
asyncQueueAddCompactEntry(Oid dbid, TransactionId xid, XLogRecPtr notify_lsn)
{
	AsyncQueueEntry entry;
	QueuePosition queue_head;
	int64		pageno;
	int64		entry_pageno = -1; /* page where the entry is written */
	int			offset;
	int			slotno;
	LWLock	   *banklock;

	/*
	 * Fill in the compact entry with just the metadata.
	 * No payload data is stored here - it's all in WAL.
	 */
	entry.dboid = dbid;
	entry.xid = xid;
	entry.notify_lsn = notify_lsn;

	/* Caller should already hold NotifyQueueLock in exclusive mode */
	queue_head = QUEUE_HEAD;

	/* Capacity was reserved in PreCommit_Notify. Just write the entry. */

	/*
	 * Get the current page. If this is the first write since postmaster
	 * started, initialize the first page.
	 */
	pageno = QUEUE_POS_PAGE(queue_head);
	banklock = SimpleLruGetBankLock(NotifyCtl, pageno);

	LWLockAcquire(banklock, LW_EXCLUSIVE);

	if (QUEUE_POS_IS_ZERO(queue_head))
		slotno = SimpleLruZeroPage(NotifyCtl, pageno);
	else
		slotno = SimpleLruReadPage(NotifyCtl, pageno, true,
								   InvalidTransactionId);

	/* Mark the page dirty before writing */
	NotifyCtl->shared->page_dirty[slotno] = true;

	offset = QUEUE_POS_OFFSET(queue_head);

	/* Check if the compact entry fits on the current page */
	if (offset + sizeof(AsyncQueueEntry) <= QUEUE_PAGESIZE)
	{
		/* Copy the compact entry to the shared buffer */
		memcpy(NotifyCtl->shared->page_buffer[slotno] + offset,
			   &entry,
			   sizeof(AsyncQueueEntry));

		entry_pageno = pageno;

		/* Advance queue head by the size of our compact entry */
		if (asyncQueueAdvance(&queue_head, sizeof(AsyncQueueEntry)))
		{
			/*
			 * Page became full. Initialize the next page to ensure SLRU
			 * consistency (similar to what asyncQueueAddEntries does).
			 */
			LWLock	   *nextlock;

			pageno = QUEUE_POS_PAGE(queue_head);
			nextlock = SimpleLruGetBankLock(NotifyCtl, pageno);
			if (nextlock != banklock)
			{
				LWLockRelease(banklock);
				LWLockAcquire(nextlock, LW_EXCLUSIVE);
			}
			SimpleLruZeroPage(NotifyCtl, QUEUE_POS_PAGE(queue_head));
			if (nextlock != banklock)
			{
				LWLockRelease(nextlock);
				LWLockAcquire(banklock, LW_EXCLUSIVE);
			}

			/* Set cleanup flag if appropriate */
			if (QUEUE_POS_PAGE(queue_head) % QUEUE_CLEANUP_DELAY == 0)
				tryAdvanceTail = true;
		}

		/* Update the global queue head and consume reservation (not in recovery) */
		QUEUE_HEAD = queue_head;
		if (!RecoveryInProgress())
		{
			Assert(asyncQueueControl->reservedEntries > 0);
			asyncQueueControl->reservedEntries--;
		}
	}
	else
	{
		/*
		 * No room on current page. Move to the next page and write entry at
		 * offset 0; padding is unnecessary with fixed-size entries and bounded
		 * scans that stop at QUEUE_HEAD.
		 */
		LWLockRelease(banklock);

		/* Move head to the start of the next page */
		SET_QUEUE_POS(queue_head, QUEUE_POS_PAGE(queue_head) + 1, 0);

		/* Ensure next page is present */
		banklock = SimpleLruGetBankLock(NotifyCtl, QUEUE_POS_PAGE(queue_head));
		LWLockAcquire(banklock, LW_EXCLUSIVE);
		slotno = SimpleLruZeroPage(NotifyCtl, QUEUE_POS_PAGE(queue_head));
		NotifyCtl->shared->page_dirty[slotno] = true;

		/* Write entry at beginning of the new page */
		memcpy(NotifyCtl->shared->page_buffer[slotno], &entry, sizeof(AsyncQueueEntry));

		entry_pageno = QUEUE_POS_PAGE(queue_head);

		/* Advance queue head and initialize subsequent page if needed */
		if (asyncQueueAdvance(&queue_head, sizeof(AsyncQueueEntry)))
		{
			LWLock *nextlock;
			pageno = QUEUE_POS_PAGE(queue_head);
			nextlock = SimpleLruGetBankLock(NotifyCtl, pageno);
			if (nextlock != banklock)
			{
				LWLockRelease(banklock);
				LWLockAcquire(nextlock, LW_EXCLUSIVE);
			}
			SimpleLruZeroPage(NotifyCtl, QUEUE_POS_PAGE(queue_head));
			if (nextlock != banklock)
			{
				LWLockRelease(nextlock);
				LWLockAcquire(banklock, LW_EXCLUSIVE);
			}
			if (QUEUE_POS_PAGE(queue_head) % QUEUE_CLEANUP_DELAY == 0)
				tryAdvanceTail = true;
		}

		/* Update the global queue head and consume reservation (not in recovery) */
		QUEUE_HEAD = queue_head;
		if (!RecoveryInProgress())
		{
			Assert(asyncQueueControl->reservedEntries > 0);
			asyncQueueControl->reservedEntries--;
		}
	}

	/* Update per-page minimum and remove from uncommitted list under locks. */
	if (entry_pageno >= 0)
	{
		/* Caller holds NotifyQueueLock EXCLUSIVE (see xact.c commit path). */
		NotifyPageMinUpdateForPage(entry_pageno, notify_lsn);
		UncommittedNotifyRemoveByFullXid(GetTopFullTransactionIdIfAny());
	}

	LWLockRelease(banklock);
}

/*
 * Advance the shared queue tail variable to the minimum of all the
 * per-backend tail pointers.  Truncate pg_notify space if possible.
 *
 * This is (usually) called during CommitTransaction(), so it's important for
 * it to have very low probability of failure.
 */
static void
asyncQueueAdvanceTail(void)
{
	QueuePosition min;
	int64		oldtailpage;
	int64		newtailpage;
	int64		boundary;

	/* Restrict task to one backend per cluster; see SimpleLruTruncate(). */
	LWLockAcquire(NotifyQueueTailLock, LW_EXCLUSIVE);

	/*
	 * Compute the new tail.  Pre-v13, it's essential that QUEUE_TAIL be exact
	 * (ie, exactly match at least one backend's queue position), so it must
	 * be updated atomically with the actual computation.  Since v13, we could
	 * get away with not doing it like that, but it seems prudent to keep it
	 * so.
	 *
	 * Also, because incoming backends will scan forward from QUEUE_TAIL, that
	 * must be advanced before we can truncate any data.  Thus, QUEUE_TAIL is
	 * the logical tail, while QUEUE_STOP_PAGE is the physical tail, or oldest
	 * un-truncated page.  When QUEUE_STOP_PAGE != QUEUE_POS_PAGE(QUEUE_TAIL),
	 * there are pages we can truncate but haven't yet finished doing so.
	 *
	 * For concurrency's sake, we don't want to hold NotifyQueueLock while
	 * performing SimpleLruTruncate.  This is OK because no backend will try
	 * to access the pages we are in the midst of truncating.
	 */
	LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
	min = QUEUE_HEAD;
	for (ProcNumber i = QUEUE_FIRST_LISTENER; i != INVALID_PROC_NUMBER; i = QUEUE_NEXT_LISTENER(i))
	{
		Assert(QUEUE_BACKEND_PID(i) != InvalidPid);
		min = QUEUE_POS_MIN(min, QUEUE_BACKEND_POS(i));
	}
	QUEUE_TAIL = min;
	oldtailpage = QUEUE_STOP_PAGE;
	LWLockRelease(NotifyQueueLock);

	/*
	 * We can truncate something if the global tail advanced across an SLRU
	 * segment boundary.
	 *
	 * XXX it might be better to truncate only once every several segments, to
	 * reduce the number of directory scans.
	 */
	newtailpage = QUEUE_POS_PAGE(min);
	boundary = newtailpage - (newtailpage % SLRU_PAGES_PER_SEGMENT);
	if (asyncQueuePagePrecedes(oldtailpage, boundary))
	{
		/*
		 * SimpleLruTruncate() will ask for SLRU bank locks but will also
		 * release the lock again.
		 */
		SimpleLruTruncate(NotifyCtl, newtailpage);

		LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
		/* Invalidate per-page mins for pages we are truncating */
		NotifyPageMinInvalidateRange(oldtailpage, newtailpage);
		QUEUE_STOP_PAGE = newtailpage;
		LWLockRelease(NotifyQueueLock);
	}

	LWLockRelease(NotifyQueueTailLock);
}

/*
 * ProcessIncomingNotify
 *
 *		Scan the queue for arriving notifications and report them to the front
 *		end.  The notifications might be from other sessions, or our own;
 *		there's no need to distinguish here.
 *
 *		If "flush" is true, force any frontend messages out immediately.
 *
 *		NOTE: since we are outside any transaction, we must create our own.
 */
static void
ProcessIncomingNotify(bool flush)
{
	/* We *must* reset the flag */
	notifyInterruptPending = false;

	/* Do nothing else if we aren't actively listening */
	if (listenChannels == NIL)
		return;

	if (Trace_notify)
		elog(DEBUG1, "ProcessIncomingNotify");

	set_ps_display("notify interrupt");

	/*
	 * We must run asyncQueueReadAllNotifications inside a transaction, else
	 * bad things happen if it gets an error.
	 */
	StartTransactionCommand();

	asyncQueueReadAllNotifications();

	CommitTransactionCommand();

	/*
	 * If this isn't an end-of-command case, we must flush the notify messages
	 * to ensure frontend gets them promptly.
	 */
	if (flush)
		pq_flush();

	set_ps_display("idle");

	if (Trace_notify)
		elog(DEBUG1, "ProcessIncomingNotify: done");
}

/*
 * Send NOTIFY message to my front end.
 */
void
NotifyMyFrontEnd(const char *channel, const char *payload, int32 srcPid)
{
	if (whereToSendOutput == DestRemote)
	{
		StringInfoData buf;

		pq_beginmessage(&buf, PqMsg_NotificationResponse);
		pq_sendint32(&buf, srcPid);
		pq_sendstring(&buf, channel);
		pq_sendstring(&buf, payload);
		pq_endmessage(&buf);

		/*
		 * NOTE: we do not do pq_flush() here.  Some level of caller will
		 * handle it later, allowing this message to be combined into a packet
		 * with other ones.
		 */
	}
	else
		elog(INFO, "NOTIFY for \"%s\" payload \"%s\"", channel, payload);
}

/* Does pendingNotifies include a match for the given event? */
static bool
AsyncExistsPendingNotify(Notification *n)
{
	if (pendingNotifies == NULL)
		return false;

	if (pendingNotifies->hashtab != NULL)
	{
		/* Use the hash table to probe for a match */
		if (hash_search(pendingNotifies->hashtab,
						&n,
						HASH_FIND,
						NULL))
			return true;
	}
	else
	{
		/* Must scan the event list */
		ListCell   *l;

		foreach(l, pendingNotifies->events)
		{
			Notification *oldn = (Notification *) lfirst(l);

			if (n->channel_len == oldn->channel_len &&
				n->payload_len == oldn->payload_len &&
				memcmp(n->data, oldn->data,
					   n->channel_len + n->payload_len + 2) == 0)
				return true;
		}
	}

	return false;
}

/*
 * Add a notification event to a pre-existing pendingNotifies list.
 *
 * Because pendingNotifies->events is already nonempty, this works
 * correctly no matter what CurrentMemoryContext is.
 */
static void
AddEventToPendingNotifies(Notification *n)
{
	Assert(pendingNotifies->events != NIL);

	/* Create the hash table if it's time to */
	if (list_length(pendingNotifies->events) >= MIN_HASHABLE_NOTIFIES &&
		pendingNotifies->hashtab == NULL)
	{
		HASHCTL		hash_ctl;
		ListCell   *l;

		/* Create the hash table */
		hash_ctl.keysize = sizeof(Notification *);
		hash_ctl.entrysize = sizeof(struct NotificationHash);
		hash_ctl.hash = notification_hash;
		hash_ctl.match = notification_match;
		hash_ctl.hcxt = CurTransactionContext;
		pendingNotifies->hashtab =
			hash_create("Pending Notifies",
						256L,
						&hash_ctl,
						HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

		/* Insert all the already-existing events */
		foreach(l, pendingNotifies->events)
		{
			Notification *oldn = (Notification *) lfirst(l);
			bool		found;

			(void) hash_search(pendingNotifies->hashtab,
							   &oldn,
							   HASH_ENTER,
							   &found);
			Assert(!found);
		}
	}

	/* Add new event to the list, in order */
	pendingNotifies->events = lappend(pendingNotifies->events, n);

	/* Add event to the hash table if needed */
	if (pendingNotifies->hashtab != NULL)
	{
		bool		found;

		(void) hash_search(pendingNotifies->hashtab,
						   &n,
						   HASH_ENTER,
						   &found);
		Assert(!found);
	}
}

/*
 * notification_hash: hash function for notification hash table
 *
 * The hash "keys" are pointers to Notification structs.
 */
static uint32
notification_hash(const void *key, Size keysize)
{
	const Notification *k = *(const Notification *const *) key;

	Assert(keysize == sizeof(Notification *));
	/* We don't bother to include the payload's trailing null in the hash */
	return DatumGetUInt32(hash_any((const unsigned char *) k->data,
								   k->channel_len + k->payload_len + 1));
}

/*
 * notification_match: match function to use with notification_hash
 */
static int
notification_match(const void *key1, const void *key2, Size keysize)
{
	const Notification *k1 = *(const Notification *const *) key1;
	const Notification *k2 = *(const Notification *const *) key2;

	Assert(keysize == sizeof(Notification *));
	if (k1->channel_len == k2->channel_len &&
		k1->payload_len == k2->payload_len &&
		memcmp(k1->data, k2->data,
			   k1->channel_len + k1->payload_len + 2) == 0)
		return 0;				/* equal */
	return 1;					/* not equal */
}

/* Clear the pendingActions and pendingNotifies lists. */
static void
ClearPendingActionsAndNotifies(void)
{
	/*
	 * Everything's allocated in either TopTransactionContext or the context
	 * for the subtransaction to which it corresponds.  So, there's nothing to
	 * do here except reset the pointers; the space will be reclaimed when the
	 * contexts are deleted.
	 */
	pendingActions = NULL;
	pendingNotifies = NULL;
}

/*
 * GUC check_hook for notify_buffers
 */
bool
check_notify_buffers(int *newval, void **extra, GucSource source)
{
	return check_slru_buffers("notify_buffers", newval);
}

/*
 * Write a WAL record containing async notification data
 *
 * This logs notification data to WAL, allowing us to release locks earlier
 * and maintain commit ordering through WAL's natural ordering guarantees.
 */
XLogRecPtr
LogAsyncNotifyData(Oid dboid, TransactionId xid, int32 srcPid,
			   uint32 nnotifications, Size data_len, char *data)
{
	xl_async_notify_data xlrec;


	xlrec.dbid = dboid;
	xlrec.xid = xid;
	xlrec.srcPid = srcPid;
	xlrec.nnotifications = nnotifications;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfAsyncNotifyData);
	XLogRegisterData(data, data_len);

	(void) XLogInsert(RM_ASYNC_ID, XLOG_ASYNC_NOTIFY_DATA);

	/* Return the begin LSN of the record we just inserted. */
	return ProcLastRecPtr;
}

/*
 * Redo function for async notification WAL records
 *
 * During recovery, we need to replay notification records. For now,
 * we'll add them to the traditional notification queue. In a complete
 * implementation, replaying backends would read directly from WAL.
 */
void
async_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_ASYNC_NOTIFY_DATA:
			/* 
			 * For notification data records, we don't need to do anything
			 * during recovery since listeners will read directly from WAL.
			 * The data is already durably stored in the WAL record itself.
			 */
			break;


		default:
			elog(PANIC, "async_redo: unknown op code %u", info);
	}
}
