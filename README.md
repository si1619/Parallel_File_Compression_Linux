# Parallel File Compression

This project provides a tool to compress multiple files in parallel using both Python and Shell scripts. It supports gzip, bzip2, and xz compression algorithms and can run benchmarks comparing sequential vs parallel performance.

## Features
- Compress files using Python or Shell
- Supports gzip, bzip2, and xz
- Uses multiprocessing or GNU Parallel
- Includes a benchmark script
- Can create test files for demo

## How to Use

### Create Sample Files
```bash
# Python
python3 scripts/parallel_compress_python.py --create-samples

# Shell
bash scripts/parallel_compress_shell.sh --create-sample

#Compress files
# Python
python3 scripts/parallel_compress_python.py sample_files/*.txt

# Shell
bash scripts/parallel_compress_shell.sh sample_files/*.txt

#Run Benchmark
bash scripts/benchmark_compression.sh 5 1

#Requirements
Python 3.6+
Bash
gzip, bzip2, xz
(Optional) GNU Parallel

#License

---



