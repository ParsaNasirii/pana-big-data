package io.github.panabigdata.maintenance.compaction;

record ParquetFileSnapshot(String name, long length, long modified) {}
