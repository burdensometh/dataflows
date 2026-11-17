import itertools
import os
import pickle
import time
import traceback
import multiprocessing as mp
import threading
import queue

from ..helpers import ResourceMatcher
from .. import PackageWrapper, ResourceWrapper


class ParallelizeError(Exception):
    """Raised when a row, worker or producer failed in strict mode."""


def _format_error(source, seq, exc):
    return '{} (row {}): {}: {}\n{}'.format(
        source,
        'unknown' if seq is None else seq,
        type(exc).__name__,
        exc,
        traceback.format_exc(),
    )


def init_mp(num_processors, row_func, q_in, q_internal):
    q_out = mp.Queue()
    processes = [mp.Process(target=work, args=(q_in, q_out, row_func)) for _ in range(num_processors)]
    for process in processes:
        process.start()
    t_fetch = threading.Thread(target=fetcher, args=(q_out, q_internal, num_processors))
    t_fetch.start()
    return (processes, t_fetch)


def fini_mp(processes, t_fetch):
    for process in processes:
        try:
            process.join(timeout=10)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
        finally:
            if hasattr(process, 'close'):
                process.close()
    t_fetch.join()


def producer(res, q_in, q_internal, num_processors, predicate):
    try:
        for row in res:
            if predicate(row):
                q_in.put(row)
            else:
                q_internal.put(row)
        for _ in range(num_processors):
            q_in.put(None)
    except Exception:
        q_internal.put(None)
        return 1
    return 0


def fetcher(q_out, q_internal, num_processors):
    expected_nones = num_processors
    while True:
        row = q_out.get()
        if row is None:
            expected_nones -= 1
            if expected_nones == 0:
                q_internal.put(None)
                break
            continue
        q_internal.put(row)


def work(q_in: mp.Queue, q_out: mp.Queue, row_func):
    pid = os.getpid()
    try:
        while True:
            row = q_in.get()
            if row is None:
                break
            try:
                row_func(row)
            except Exception as e:
                print(pid, 'FAILED TO RUN row_func {}\n'.format(e))
                pass
            q_out.put(row)
    except Exception:
        pass
    finally:
        q_out.put(None)


def fork(res, row_func, num_processors, predicate):
    predicate = predicate or (lambda x: True)
    for row in res:
        if predicate(row):
            res = itertools.chain([row], res)
            q_in = mp.Queue()
            q_internal = queue.Queue()
            t_prod = threading.Thread(target=producer, args=(res, q_in, q_internal, num_processors, predicate))
            t_prod.start()

            processes, t_fetch = init_mp(num_processors, row_func, q_in, q_internal)

            while True:
                row = q_internal.get()
                if row is None:
                    break
                yield row
            t_prod.join()
            fini_mp(processes, t_fetch)
        else:
            yield row


# ---------------------------------------------------------------------------
# Strict mode: ordered, fail-fast, bounded read-ahead
# ---------------------------------------------------------------------------

RESULT_OK = 'ok'
RESULT_ERROR = 'error'
RESULT_FATAL = 'fatal'

# How often producer/fetcher/workers check whether they were asked to stop.
POLL = 0.1
# Time budget for cooperative shutdown before threads/processes are forced.
SHUTDOWN_GRACE = 1.0
# Once every worker has exited, its queue is flushed; if results are still
# missing after this quiet period they were lost (e.g. os._exit in a worker).
LOST_RESULTS_GRACE = 1.0


def _put(q, item, stop_event):
    """Put an item, but never block shutdown for longer than POLL."""
    while not stop_event.is_set():
        try:
            q.put(item, timeout=POLL)
            return True
        except queue.Full:
            continue
    return False


def _dumps(obj):
    """Serialize eagerly so a bad payload is reported to the sender.

    ``multiprocessing.Queue`` serializes in a background feeder thread and
    silently discards objects that fail to pickle; the corresponding result
    would then never arrive and the consumer would wait forever.
    """
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


class _OrderedResults:
    """In-order hand-off of processed rows to the consumer.

    Workers finish out of order and pass-through rows are inserted by the
    producer, but `get()` only ever returns the next sequence number, so the
    source order is preserved. Errors are sticky and surface before any row
    that would come after the failure.
    """

    def __init__(self):
        self._cv = threading.Condition()
        self._items = {}
        self._total = None
        self._error = None

    def put(self, seq, row):
        with self._cv:
            self._items[seq] = row
            self._cv.notify_all()

    def fail(self, message):
        with self._cv:
            if self._error is None:
                self._error = message
            self._cv.notify_all()

    def has_error(self):
        with self._cv:
            return self._error is not None

    def finish(self, total):
        with self._cv:
            self._total = total
            self._cv.notify_all()

    def get(self, seq):
        with self._cv:
            while True:
                if self._error is not None:
                    raise ParallelizeError(self._error)
                if seq in self._items:
                    return self._items.pop(seq)
                if self._total is not None and seq >= self._total:
                    raise StopIteration()
                self._cv.wait()


class _StrictRun:
    """A parallel section of one resource which delivers rows in source order.

    A slot is taken before each row is read from the source and released only
    once that row has been handed to the consumer. This bounds the number of
    rows that were read but not yet delivered (and therefore every queue and
    buffer in the pipeline), so a slow first row cannot make memory grow
    without limit.
    """

    def __init__(self, first_row, iterator, row_func, num_processors, predicate, max_in_flight):
        self.first_row = first_row
        self.iterator = iterator
        self.row_func = row_func
        self.num_processors = num_processors
        self.predicate = predicate
        self.max_in_flight = max_in_flight
        self.results = _OrderedResults()
        self.slots = threading.Semaphore(max_in_flight)
        # Set by the producer on completion; only read by the fetcher thread.
        self.total = None
        self.n_processed = 0
        self.received_ok = 0

        self.stop_event = mp.Event()
        # Rows are pickled explicitly before being queued: multiprocessing.Queue
        # silently drops an unpicklable object (its feeder thread logs a
        # traceback and moves on), which would leave the consumer waiting for a
        # result forever. Doing it inline turns that into a normal error.
        self.q_in = mp.Queue()
        self.q_out = mp.Queue()
        self.processes = []
        self.t_prod = None
        self.t_fetch = None
        self.closed = False

    def start(self):
        for _ in range(self.num_processors):
            process = mp.Process(
                target=work_strict,
                args=(self.q_in, self.q_out, self.row_func, self.stop_event),
            )
            process.daemon = True
            process.start()
            self.processes.append(process)
        self.t_prod = threading.Thread(target=self._produce, name='parallelize-producer')
        self.t_prod.daemon = True
        self.t_prod.start()
        self.t_fetch = threading.Thread(target=self._fetch, name='parallelize-fetcher')
        self.t_fetch.daemon = True
        self.t_fetch.start()

    def _acquire_slot(self):
        while not self.stop_event.is_set():
            if self.slots.acquire(timeout=POLL):
                return True
        return False

    def _produce(self):
        seq = 0
        n_processed = 0
        finished = False
        have_first = True
        try:
            # The first row was already read (and matched the predicate) by
            # fork_strict, so it is not read or tested again here; the slot is
            # still taken before it is handed off, keeping read-ahead bounded.
            first_row = self.first_row
            iterator = iter(self.iterator)
            while True:
                # Take the slot *before* reading, so at most max_in_flight rows
                # are ever read without having been delivered.
                if not self._acquire_slot():
                    return
                if have_first:
                    row = first_row
                    have_first = False
                else:
                    try:
                        row = next(iterator)
                    except StopIteration:
                        break
                    if self.stop_event.is_set():
                        return
                    if not self.predicate(row):
                        # Pass-through rows keep their original position.
                        self.results.put(seq, row)
                        seq += 1
                        continue
                if self.stop_event.is_set():
                    return
                try:
                    message = _dumps((seq, row))
                except Exception as e:
                    self.results.fail(_format_error('row could not be serialized for a worker', seq, e))
                    self.stop_event.set()
                    return
                if not _put(self.q_in, message, self.stop_event):
                    return
                n_processed += 1
                seq += 1
            self.n_processed = n_processed
            self.total = seq
            self.results.finish(seq)
            for _ in range(self.num_processors):
                _put(self.q_in, None, self.stop_event)
            finished = True
        except Exception as e:
            self.results.fail(_format_error('producer failed', None, e))
            self.stop_event.set()
        finally:
            if not finished and not self.stop_event.is_set():
                self.results.fail('producer terminated unexpectedly')
                self.stop_event.set()

    def _process_exitcode(self, process):
        try:
            return process.exitcode
        except (ValueError, AttributeError):
            # close() may already have released the process handle.
            return None

    def _check_workers(self, idle_since):
        """Detect crashed workers and results lost on the way back.

        Returns ``(idle_since, done)``: the updated timestamp of the current
        idle period, and whether the fetcher can stop (all results received or
        an error recorded).
        """
        for process in self.processes:
            exitcode = self._process_exitcode(process)
            if exitcode is None or self.results.has_error():
                continue
            if exitcode != 0:
                message = 'worker process {} failed with exit code {}'.format(process.pid, exitcode)
            elif self.stop_event.is_set():
                # Another worker has already failed; its error message is on
                # its way and is the more useful one to report.
                continue
            elif self.total is None:
                # Workers only exit on their own once the producer finished and
                # sent the sentinels, which happens after `total` is published.
                # A clean exit before that means the worker died mid-run (e.g.
                # sys.exit(0) called inside row_func).
                message = 'worker process {} exited unexpectedly'.format(process.pid)
            else:
                continue
            self.results.fail(message)
            self.stop_event.set()
            return None, True
        if self.results.has_error():
            return None, True
        if self.total is None:
            # The producer is still reading rows; results may still arrive.
            return idle_since, False
        try:
            all_exited = not any(process.is_alive() for process in self.processes)
        except (ValueError, AttributeError):
            return idle_since, False
        if self.received_ok >= self.n_processed:
            # Everything the workers produced has been received.
            return None, all_exited
        if not all_exited:
            return idle_since, False
        # The producer is done and cleanly exiting workers flush their queue
        # before dying; if nothing has arrived for a while, results were lost.
        now = time.monotonic()
        if idle_since is None:
            return now, False
        if now - idle_since >= LOST_RESULTS_GRACE:
            self.results.fail(
                'worker processes exited before delivering all rows '
                '({} of {} processed rows received)'.format(self.received_ok, self.n_processed))
            self.stop_event.set()
            return None, True
        return idle_since, False

    def _fetch(self):
        # Do not simply stop on stop_event: a worker sets it right after
        # putting a failure message, and that message must still reach the
        # consumer. We keep draining the queue until it is quiet, then make
        # sure *some* error is recorded so the consumer never waits forever.
        idle_since = None
        drain_deadline = None
        while not self.closed:
            try:
                item = self.q_out.get(timeout=POLL)
            except queue.Empty:
                if self.stop_event.is_set():
                    if drain_deadline is None:
                        drain_deadline = time.monotonic() + SHUTDOWN_GRACE
                    if self.results.has_error():
                        return
                    if time.monotonic() >= drain_deadline:
                        self.results.fail('parallelize stopped before all rows were delivered')
                        return
                    continue
                idle_since, done = self._check_workers(idle_since)
                if done:
                    return
                continue
            except (EOFError, OSError) as e:
                self.results.fail('failed to read results from workers: {}'.format(e))
                self.stop_event.set()
                return
            idle_since = None
            try:
                seq, kind, payload = pickle.loads(item)
            except Exception as e:
                self.results.fail('failed to read a result from a worker: {}'.format(e))
                self.stop_event.set()
                return
            if kind == RESULT_OK:
                self.received_ok += 1
                self.results.put(seq, payload)
            else:  # RESULT_ERROR or RESULT_FATAL
                self.results.fail(payload)
                self.stop_event.set()
                return

    def __iter__(self):
        seq = 0
        while True:
            try:
                row = self.results.get(seq)
            except StopIteration:
                return
            yield row
            self.slots.release()
            seq += 1

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.stop_event.set()
        self.results.fail('parallelize stopped before all rows were delivered')

        deadline = time.monotonic() + SHUTDOWN_GRACE
        for thread in (self.t_prod, self.t_fetch):
            if thread is not None:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))

        for process in self.processes:
            process.join(timeout=max(0.0, deadline - time.monotonic()))
        lingering = [process for process in self.processes if process.is_alive()]
        for process in lingering:
            try:
                process.terminate()
            except Exception:
                pass
        deadline = time.monotonic() + SHUTDOWN_GRACE
        for process in lingering:
            process.join(timeout=max(0.0, deadline - time.monotonic()))
        for process in lingering:
            if process.is_alive():
                try:
                    process.kill()
                except Exception:
                    pass
                process.join(timeout=SHUTDOWN_GRACE)
        for process in self.processes:
            if hasattr(process, 'close'):
                try:
                    process.close()
                except Exception:
                    pass

        for q in (self.q_in, self.q_out):
            try:
                q.cancel_join_thread()
            except Exception:
                pass
            try:
                q.close()
            except Exception:
                pass


def work_strict(q_in: mp.Queue, q_out: mp.Queue, row_func, stop_event):
    try:
        while not stop_event.is_set():
            try:
                item = q_in.get(timeout=POLL)
            except queue.Empty:
                continue
            if item is None:
                break
            if stop_event.is_set():
                break
            seq, row = pickle.loads(item)
            try:
                row_func(row)
            except Exception as e:
                _put(q_out, _dumps((seq, RESULT_ERROR, _format_error('row_func failed', seq, e))), stop_event)
                stop_event.set()
                break
            try:
                # The processed row is sent back, not the original one.
                message = _dumps((seq, RESULT_OK, row))
            except Exception as e:
                err = _format_error('row_func returned an unserializable row', seq, e)
                _put(q_out, _dumps((seq, RESULT_ERROR, err)), stop_event)
                stop_event.set()
                break
            if not _put(q_out, message, stop_event):
                break
    except Exception as e:
        try:
            q_out.put(_dumps((None, RESULT_FATAL, _format_error('worker failed', None, e))), timeout=POLL)
        except Exception:
            pass
        stop_event.set()


def fork_strict(res, row_func, num_processors, predicate, max_in_flight):
    predicate = predicate or (lambda x: True)
    iterator = iter(res)
    for row in iterator:
        if not predicate(row):
            # Rows which don't match the predicate keep their original place.
            yield row
            continue
        run = _StrictRun(row, iterator, row_func, num_processors, predicate, max_in_flight)
        try:
            run.start()
            yield from run
        finally:
            run.close()
        return


def parallelize(row_func, num_processors=None, resources=None, predicate=None,
                strict=False, max_in_flight=None):
    num_processors = num_processors or 2*os.cpu_count()
    if not isinstance(num_processors, int) or isinstance(num_processors, bool) or num_processors < 1:
        raise ValueError('num_processors must be a positive integer')
    if max_in_flight is not None:
        if not isinstance(max_in_flight, int) or isinstance(max_in_flight, bool) or max_in_flight < 1:
            raise ValueError('max_in_flight must be a positive integer')
        if not strict:
            raise ValueError('max_in_flight is only supported with strict=True')
    if strict and max_in_flight is None:
        # Keep every worker fed while still bounding the read-ahead.
        max_in_flight = 2 * num_processors

    def func(package: PackageWrapper):
        yield package.pkg
        matcher = ResourceMatcher(resources, package.pkg)

        res: ResourceWrapper
        for res in package:
            if matcher.match(res.res.name):
                if strict:
                    yield fork_strict(res, row_func, num_processors, predicate, max_in_flight)
                else:
                    yield fork(res, row_func, num_processors, predicate)
            else:
                yield res

    return func
