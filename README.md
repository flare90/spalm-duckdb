# Spalm

## Preliminaries
Follow the steps in https://github.com/flare90/spalm and set environment variables for Intel oneMKL, and build shared libary spalm.so.

Open src/physical_spalm.cpp, and edit line 195 to point to spalm.so.

Then, copy the directory 'parameters' from your 'spalm' directory into this directory ('spalm-duckdb').


### Build steps
To build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/extension/spalm/spalm.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `spalm.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the example 'mm_test.sql' with Spalm, run
```
./build/release/duckdb < mm_test.sql
```

