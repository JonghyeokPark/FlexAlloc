# How to install

**Install**I
Copy rocksdb_plugins/flexalloc to ROCKSDB_ROOT/plugin/. At the end of this step
you should have a directory structure like this
ROCKSDB_ROOT/plugin/flexalloc/flexalloc.{cc,h,mk}

**Compile**
Compile db_bench with the following line:
 ```
 DEBUG_LEVEL=0 ROCKSDEB_PLUGINS="flexalloc" make -j16 db_bench
 ```
**Run db_bench**
Use the following constrcut to execute the db_bench command:
```
sudo ./db_bench --benchmarks=fillseq --env_uri=flexalloc --num=5
```
The important part here is to pass "flexalloc" as an env_uri


