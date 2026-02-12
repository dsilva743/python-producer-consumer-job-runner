<h1 align="center">🧵 Python Producer–Consumer Job Runner</h1>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.12-blue">
  <img src="https://img.shields.io/badge/platform-macOS-lightgrey">
  <img src="https://img.shields.io/badge/concurrency-threading-green">
  <img src="https://img.shields.io/badge/pattern-producer--consumer-orange">
  <img src="https://img.shields.io/badge/failures-simulated-red">
<img src="https://img.shields.io/badge/status-learning--project-purple">
</p>

<p align="center">
Concurrent job processing system using producer–consumer patterns, thread pools, simulated failures, retries, and live metrics monitoring.
</p>

---

## Overview

This project is a concurrent job processing system built in Python to practice real-world threading, worker pools, and queue coordination.

A filesystem watcher produces jobs when files are created. Jobs are placed into a bounded queue, dispatched by a coordinator thread, and executed by a ThreadPoolExecutor worker pool. The system includes retry logic with **simulated random failures** and a background metrics monitor that reports runtime statistics.

This project was built as a learning exercise to understand how job runners and worker systems operate internally.

---

## Features

- Producer–consumer architecture
- Watchdog file event producer
- Thread-safe bounded queue
- Dispatcher thread
- ThreadPoolExecutor workers
- Retry logic with simulated failures
- Thread-safe shared metrics
- Live metrics monitor thread
- Throughput + backlog reporting


## Limitations

- Failure behavior is simulated for testing
- Shutdown is interrupt-based (Ctrl+C), not fully graceful
