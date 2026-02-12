from collections import defaultdict
from datetime import datetime, UTC
import os
import threading
import time
import random
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent
from faker import Faker
import uuid
from enum import Enum


fake = Faker()


# =========================
# Status Enum
# =========================

class Status(Enum):
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"


# =========================
# Job Model
# =========================

class Job:
    def __init__(self, file_path: str, retries: int = 2):
        self._id = uuid.uuid4()
        self._file_path = file_path
        self._submitted_at = datetime.now(UTC)
        self._started_at = None
        self._finished_at = None
        self._status: Status | None = None
        self._retry_attempted = 0
        self._max_retries = retries

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def file_path(self) -> str:
        return self._file_path

    @property
    def status(self) -> Status | None:
        return self._status

    @property
    def max_retries(self) -> int:
        return self._max_retries

    @property
    def retry_attempted(self) -> int:
        return self._retry_attempted

    @retry_attempted.setter
    def retry_attempted(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("retry_attempted must be int")
        self._retry_attempted = value

    def mark_started(self) -> None:
        self._started_at = datetime.now(UTC)
        self._status = Status.RUNNING

    def mark_completed(self) -> None:
        self._finished_at = datetime.now(UTC)
        self._status = Status.COMPLETED

    def mark_failed(self) -> None:
        self._finished_at = datetime.now(UTC)
        self._status = Status.FAILED

    @property
    def duration(self) -> float | None:
        if self._started_at and self._finished_at:
            return (self._finished_at - self._started_at).total_seconds()
        return None


# =========================
# Watchdog Producer
# =========================

class MyHandler(FileSystemEventHandler):
    def __init__(self, queue: Queue[Job]):
        self.queue = queue

    def on_created(self, event: FileCreatedEvent) -> None:
        if not event.is_directory:
            name = os.path.basename(event.src_path)
            print(f"[PRODUCER] enqueue {name}")
            self.queue.put(Job(event.src_path))


# =========================
# Worker Engine
# =========================

class Worker:
    def __init__(self, queue: Queue[Job], status: dict[str, int], lock: Lock, max_workers: int = 4):
        self.queue = queue
        self.job_status = status
        self.status_lock = lock
        self.stop_event = Event()
        self.worker_executor = ThreadPoolExecutor(max_workers=max_workers)

    # ---------- dispatcher ----------

    def dispatch_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                # optional throttle so you can see buffering
                time.sleep(random.uniform(0.1, 0.5))
                job = self.queue.get(timeout=1)
            except Empty:
                continue

            self.worker_executor.submit(self._execute_with_retry, job)

        self.worker_executor.shutdown(wait=True)

    # ---------- actual job ----------

    def _run_job(self, job: Job) -> None:
        job.mark_started()

        print(
            f"[ WORKER START] file={os.path.basename(job.file_path):<12} "
            f"id={str(job.id)[:8]}"
        )

        try:
            line_count = 0
            word_count = 0

            # simulate slow work
            time.sleep(random.uniform(1.0, 2.5))

            with open(job.file_path, "r") as f:
                for line in f:
                    line_count += 1
                    word_count += len(line.split())

            print(
                f"[ WORKER DONE ] file={os.path.basename(job.file_path):<12} "
                f"lines={line_count:<7} words={word_count:<7}"
            )

        except Exception as e:
            job.mark_failed()
            with self.status_lock:
                self.job_status[Status.FAILED.value] += 1
                self.job_status[Status.RUNNING.value] -= 1
            print(f"[ERROR] file={job.file_path} err={e}")
            raise

    # ---------- retry wrapper ----------

    def _execute_with_retry(self, job: Job):

        # mark running once per job lifecycle
        with self.status_lock:
            self.job_status[Status.RUNNING.value] += 1

        for _ in range(job.max_retries + 1):
            failure_probability = random.uniform(0.10, 0.75)

            if random.random() <= failure_probability:
                job.retry_attempted += 1
                print(
                    f"[RETRY] file={os.path.basename(job.file_path):<12} "
                    f"attempt={job.retry_attempted}"
                )

                if job.retry_attempted >= job.max_retries:
                    job.mark_failed()
                    with self.status_lock:
                        self.job_status[Status.FAILED.value] += 1
                        self.job_status[Status.RUNNING.value] -= 1
                    print(f"[FAILED] file={os.path.basename(job.file_path):<12}")
                    return

                continue

            # success path
            self._run_job(job)

            job.mark_completed()
            with self.status_lock:
                self.job_status[Status.COMPLETED.value] += 1
                self.job_status[Status.RUNNING.value] -= 1
            return


# =========================
# File Generator
# =========================

def create_files(
    out_dir: str = "inputs",
    file_count: int = 50,
    lines_per_file: int = 20_000,
    producer_delay: tuple[float, float] = (0.05, 0.12),
):
    for i in range(file_count):
        path = os.path.join(out_dir, f"file_{i}.txt")

        with open(path, "w") as f:
            for _ in range(lines_per_file):
                f.write(fake.sentence() + "\n")

            f.flush()
            os.fsync(f.fileno())

        time.sleep(random.uniform(*producer_delay))




# =========================
# Metrics Monitor Thread
# =========================

def metrics_loop(worker, job_queue, interval=2.0):
    last_completed = 0
    last_time = time.time()

    while not worker.stop_event.is_set():
        time.sleep(interval)

        with worker.status_lock:
            running = worker.job_status[Status.RUNNING.value]
            completed = worker.job_status[Status.COMPLETED.value]
            failed = worker.job_status[Status.FAILED.value]

        queued = job_queue.qsize()
        exec_backlog = worker.worker_executor._work_queue.qsize()

        now = time.time()
        delta = completed - last_completed
        throughput = delta / (now - last_time) if now > last_time else 0
        last_completed = completed
        last_time = now

        print("========== METRICS ==========")
        print(f"queued_buffer   : {queued}")
        print(f"running         : {running}")
        print(f"completed       : {completed}")
        print(f"failed          : {failed}")
        print(f"executor_backlog: {exec_backlog}")
        print(f"throughput/sec  : {throughput:.2f}")
        print("=============================")




# =========================
# Main
# =========================

if __name__ == "__main__":
    job_queue = Queue(maxsize=15)

    job_status = defaultdict(int)
    status_lock = Lock()

    os.makedirs("inputs", exist_ok=True)

    # file producer
    file_generator = threading.Thread(target=create_files, name="generator")
    file_generator.start()

    # worker system
    worker = Worker(job_queue, job_status, status_lock, max_workers=4)
    dispatcher_thread = threading.Thread(target=worker.dispatch_loop, name="dispatcher")
    dispatcher_thread.start()

    # metrics thread
    metrics_thread = threading.Thread(
        target=metrics_loop,
        args=(worker, job_queue),
        name="metrics",
        daemon=True,
    )
    metrics_thread.start()

    # watchdog producer
    handler = MyHandler(job_queue)
    observer = Observer()
    observer.schedule(handler, "./inputs", recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutdown...")
        observer.stop()
        worker.stop_event.set()

    observer.join()
    dispatcher_thread.join()

