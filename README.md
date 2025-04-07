# BitTorrent Tracker

This project implements a multi-threaded, peer-to-peer (P2P) file sharing protocol using MPI and pthreads. It simulates the behavior of a BitTorrent-style tracker-client system with optimized workload balancing and decentralized data exchange.

## Features

- **Tracker Logic**: Central coordinator that receives file hashes, manages swarms, and tracks download/upload states.
- **Client Logic**: Each client performs concurrent upload/download using separate threads and communicates with both the tracker and other peers.
- **Load Balancing**: Implements a dynamic `use_count` system to pick the least-burdened uploader during download.
- **Swarm Updates**: Periodic synchronization with tracker after every 10 segments to ensure up-to-date swarm distribution.
- **Termination Logic**: Tracker automatically shuts down once all clients complete downloads.

## Structure

- `tracker()` — Coordinates swarm data, hash registry, and message parsing.
- `peer()` — Spawns download/upload threads, manages local file registry.
- `download_thread_func()` — Handles segment requests, updates, file assembly.
- `upload_thread_func()` — Responds to incoming segment requests and tracks usage.
- `parse_input_file()` — Parses each client's input configuration.

## Technologies Used
- C
- MPI (with `MPI_THREAD_MULTIPLE`)
- POSIX Threads (`pthreads`)
- File I/O & Synchronization

## Run Instructions

1. Prepare input files: `in1.txt`, `in2.txt`, etc., each describing owned and desired files.
2. Compile with MPI:
   ```bash
   mpic++ -pthread main.cpp -o p2p
3. Launch with N clients + 1 tracker:
    ```bash
    mpirun -np <N+1> ./p2p

