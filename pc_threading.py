"""
This is the written comparison portion of the homework assignment:



I've created the same producer/consumer system two times, with three producers,
three consumers, and a buffer that can hold five items. In the threading
version the buffer is simply a Python list with a single lock and two
condition variables (one for "not full" and one for "not empty"), and all the
threads are using it directly.

In the case of multiprocessing, since processes do not share the memory, I used
multiprocessing.Queue instead. It serializes every item and sends it through a
pipe to other processes. The stopping of both implementations is identical. As
soon as the producers are finished, the main thread sends a None for each
consumer and closes everything. In every run, the amount of items produced equals
to the number of items consumed, the buffer is empty, and all processes are
finished.

For the I/O bound test, threading took 0.061 s and multiprocessing took 0.077 s,
so they were pretty close. The GIL lets go when a thread is sleeping or waiting,
so the threads overlap just fine. Multiprocessing was a bit slower since starting
processes and sending items through a pipe takes extra time.

For the CPU bound test, threading took 0.314 s and multiprocessing took 0.249 s.
The GIL only lets one thread run Python code at a time, so my threads couldn't
really do the math at the same time. Each process gets its own interpreter and its
own GIL, so they can use different cores. Multiprocessing ended up about 21%
faster here, which is basically what the GIL cost the threaded version. It barely
mattered for the I/O bound one though. Overall threads are easier and start
faster, but processes are better when it's mostly heavy math.
"""
import argparse
import random
import threading
import time

SENTINEL = None  


def do_work(kind):
    """Simulated per-item work. io = sleep (GIL released), cpu = heavy math."""
    if kind == "io":
        time.sleep(random.uniform(0.001, 0.01))
    else:
        sum(i * i for i in range(200_000))


class BoundedBuffer:
    """Fixed-size FIFO buffer: one lock, two conditions (not_full / not_empty)."""

    def __init__(self, capacity):
        self.capacity = capacity
        self.items = []
        lock = threading.Lock()
        self.not_full = threading.Condition(lock)   
        self.not_empty = threading.Condition(lock)  

    def put(self, item, name, log):
        with self.not_full:
            while len(self.items) >= self.capacity:      
                log(f"[{name}] buffer full - waiting")
                self.not_full.wait()
                log(f"[{name}] resumed")
            self.items.append(item)                      
            size = len(self.items)
            self.not_empty.notify()
        return size

    def get(self):
        with self.not_empty:
            while not self.items:
                self.not_empty.wait()
            item = self.items.pop(0)
            size = len(self.items)
            self.not_full.notify()
        return item, size

    def size(self):
        with self.not_empty:
            return len(self.items)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", "--producers", type=int, default=3)
    ap.add_argument("-c", "--consumers", type=int, default=3)
    ap.add_argument("-n", "--capacity", type=int, default=5)
    ap.add_argument("-i", "--items", type=int, default=10, help="items per producer")
    ap.add_argument("-w", "--work", choices=["io", "cpu"], default="io")
    ap.add_argument("-q", "--quiet", action="store_true", help="suppress per-item output")
    args = ap.parse_args()

    buf = BoundedBuffer(args.capacity)
    print_lock = threading.Lock()  
    count_lock = threading.Lock()
    totals = {"produced": 0, "consumed": 0}
# Prints messages without mixing up the output
    def log(msg):
        if not args.quiet:
            with print_lock:
                print(msg, flush=True)

    def producer(pid):
        name = f"producer {pid}"
        for k in range(args.items):
            do_work(args.work)
            size = buf.put((pid, k), name, log)
            with count_lock:
                totals["produced"] += 1
            log(f"[{name}] produced item {k}      buffer size: {size}")

    def consumer(cid):
        name = f"consumer {cid}"
        while True:
            item, size = buf.get()
            if item is SENTINEL:
                log(f"[{name}] received stop signal - exiting")
                return
            do_work(args.work)
            with count_lock:
                totals["consumed"] += 1
            log(f"[{name}] consumed item {item[1]} (from producer {item[0]})      buffer size: {size}")

    start = time.perf_counter()
    producers = [threading.Thread(target=producer, args=(i + 1,)) for i in range(args.producers)]
    consumers = [threading.Thread(target=consumer, args=(i + 1,)) for i in range(args.consumers)]
    # Start all threads

    for t in producers + consumers:
        t.start()

    for t in producers:          
        t.join()

         # Send a stop signal to each consumer
    for _ in consumers:          
        buf.put(SENTINEL, "main", log)
    for t in consumers:
        t.join()
    elapsed = time.perf_counter() - start

    left = buf.size()
    print(f"all producers finished, all consumers finished, {left} items left")
    print(f"produced={totals['produced']} consumed={totals['consumed']} "
          f"expected={args.producers * args.items}")
    assert totals["produced"] == totals["consumed"] == args.producers * args.items
    assert left == 0
    print(f"[threading, {args.work}] elapsed: {elapsed:.3f} s")


if __name__ == "__main__":
    main()
