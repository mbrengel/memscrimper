# Memscrimper C++ Implementation

## Dependencies
- Boost Installation
- libbz2.so (libz2.a for static build)
- libz.so (libz.a for static build)

## Building
### Dynamically-Linked Version
```bash
mkdir build_folder && cd build_folder
cmake .. && make
```
### Statically-Linked Version
```bash
mkdir build_folder && cd build_folder
cmake .. -DSTATIC_BUILD=ON && make
```

## Example Usage
Start the MemScrimper server as follows:
```bash
./memscrimper s <thread_count> /tmp/mscr.socket
```
Sending requests via Python module:
```python
import mscr_client

pagesize = 4096
# creating client
mscr = mscr_client.MscrClient("/tmp/mscr.socket", True)

# adding reference dump (for faster compression)
mscr.add_referencedump("path/to/refdump", pagesize)

# send compression request
mscr.compress_dump("path/to/refdump", "path/to/sourcedump", "dump.compress",
                    pagesize, True, True, Compression.ZIP7)
                    
# send decompression request
mscr.decompress_dump("dump.compress", "dump.uncompress")

# remove reference dump from server memory (optional)
mscr.del_referencedump("path/to/refdump")
```




