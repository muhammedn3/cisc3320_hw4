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
import multiprocessing as mp
import random
import time

SENTINEL = None 


def do_work(kind):
    if kind == "io":
        time.sleep(random.uniform(0.001, 0.01))
    else:
        sum(i * i for i in range(200_000))


def producer(pid, buf, args, print_lock, produced):
    name = f"producer {pid}"
    for k in range(args.items):
        do_work(args.work)
        if buf.full() and not args.quiet:
            with print_lock:
                print(f"[{name}] buffer full - waiting", flush=True)
        buf.put((pid, k))              
        with produced.get_lock():
            produced.value += 1
        if not args.quiet:
            with print_lock:
                print(f"[{name}] produced item {k}      buffer size: ~{buf.qsize()}", flush=True)


def consumer(cid, buf, args, print_lock, consumed):
    name = f"consumer {cid}"
    while True:
        item = buf.get()         # Waits if the buffer is empty      
        if item is SENTINEL:
            if not args.quiet:
                with print_lock:
                    print(f"[{name}] received stop signal - exiting", flush=True)
            return
        do_work(args.work)
        with consumed.get_lock():
            consumed.value += 1
        if not args.quiet:
            with print_lock:
                print(f"[{name}] consumed item {item[1]} (from producer {item[0]})      "
                      f"buffer size: ~{buf.qsize()}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", "--producers", type=int, default=3)
    ap.add_argument("-c", "--consumers", type=int, default=3)
    ap.add_argument("-n", "--capacity", type=int, default=5)
    ap.add_argument("-i", "--items", type=int, default=10, help="items per producer")
    ap.add_argument("-w", "--work", choices=["io", "cpu"], default="io")
    ap.add_argument("-q", "--quiet", action="store_true")
    args = ap.parse_args()

    buf = mp.Queue(maxsize=args.capacity)  # Creates the shared buffer  
    print_lock = mp.Lock()
    produced = mp.Value("i", 0)         # Keeps track of produced items     
    consumed = mp.Value("i", 0)

    start = time.perf_counter()
    producers = [mp.Process(target=producer, args=(i + 1, buf, args, print_lock, produced))
                 for i in range(args.producers)]
    consumers = [mp.Process(target=consumer, args=(i + 1, buf, args, print_lock, consumed))
                 for i in range(args.consumers)]

     # Start all the processes
    for p in producers + consumers:
        p.start()

    for p in producers:
        p.join()

        # Send a stop signal to each consumer
    for _ in consumers:                     
        buf.put(SENTINEL)
    for c in consumers:
        c.join()
    elapsed = time.perf_counter() - start

    left = 0
    while not buf.empty():                  
        buf.get()
        left += 1
    print(f"all producers finished, all consumers finished, {left} items left")
    print(f"produced={produced.value} consumed={consumed.value} "
          f"expected={args.producers * args.items}")
    assert produced.value == consumed.value == args.producers * args.items
    assert left == 0
    print(f"[multiprocessing, {args.work}] elapsed: {elapsed:.3f} s")


if __name__ == "__main__":
    main()
