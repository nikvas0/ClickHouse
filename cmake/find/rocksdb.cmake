# if (USE_ROCKSDB)
find_library(ROCKSDB_LIBRARY rocksdb)

find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h PATHS ${ROCKSDB_INCLUDE_PATHS})