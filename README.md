# zero-store (WIP)
[![ZeroStoreCI](https://github.com/SarthakMakhija/zero-store/actions/workflows/build.yml/badge.svg)](https://github.com/SarthakMakhija/zero-store/actions/workflows/build.yml)

Zero-Store is a hybrid key/value store that combines the performance of in-memory operations with the durability of object storage.

The trello is available [here](https://trello.com/b/7NCeR6uX/zero-store).

Below image shows the high-level architecture.

<img width="894" alt="zero-store architecture" src="https://github.com/user-attachments/assets/ec11c076-fe3c-4af6-9e6d-4b90624a5a2e" />

### Key Features
- **Hybrid Architecture**: Leverages in-memory structures for low-latency operations while persisting data to object storage for durability.
- **Sorted In-Memory Structures**: Utilizes sorted structures (e.g., skiplists) to organize data for fast access and efficient flushing.
- **Immutable Segments**: Writes are converted into immutable segment files for faster reads.
- **Seamless Object Storage Integration**: Supports S3 as the primary durability layer.
- **Write-Optimized**: Flushes writes to object storage in sequential menner.
- **Advanced Caching**:
  - Key/Value Cache for frequent lookups.
  - Bloom Filter Cache to avoid unnecessary I/O.
  - Segment Metadata Cache for faster segment access.
- **Efficient Compaction**: Periodic merging of segments optimizes space usage and read performance.


