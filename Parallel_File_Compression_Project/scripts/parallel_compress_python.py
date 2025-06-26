#!/usr/bin/env python3
"""
Parallel File Compression using Python multiprocessing
Supports multiple compression algorithms: gzip, bzip2, lzma/xz
"""

import os
import sys
import time
import gzip
import bz2
import lzma
import argparse
from pathlib import Path
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import shutil

def compress_file_gzip(file_path, output_dir=None, compression_level=6):
    """Compress a single file using gzip"""
    try:
        input_path = Path(file_path)
        if not input_path.exists():
            return f"Error: {file_path} does not exist"
        
        if output_dir:
            output_path = Path(output_dir) / f"{input_path.name}.gz"
        else:
            output_path = input_path.with_suffix(input_path.suffix + '.gz')
        
        start_time = time.time()
        
        with open(input_path, 'rb') as f_in:
            with gzip.open(output_path, 'wb', compresslevel=compression_level) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        end_time = time.time()
        original_size = input_path.stat().st_size
        compressed_size = output_path.stat().st_size
        compression_ratio = (1 - compressed_size / original_size) * 100
        
        return {
            'file': str(input_path),
            'output': str(output_path),
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio,
            'time_taken': end_time - start_time,
            'algorithm': 'gzip'
        }
    except Exception as e:
        return f"Error compressing {file_path}: {str(e)}"

def compress_file_bzip2(file_path, output_dir=None, compression_level=9):
    """Compress a single file using bzip2"""
    try:
        input_path = Path(file_path)
        if not input_path.exists():
            return f"Error: {file_path} does not exist"
        
        if output_dir:
            output_path = Path(output_dir) / f"{input_path.name}.bz2"
        else:
            output_path = input_path.with_suffix(input_path.suffix + '.bz2')
        
        start_time = time.time()
        
        with open(input_path, 'rb') as f_in:
            with bz2.open(output_path, 'wb', compresslevel=compression_level) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        end_time = time.time()
        original_size = input_path.stat().st_size
        compressed_size = output_path.stat().st_size
        compression_ratio = (1 - compressed_size / original_size) * 100
        
        return {
            'file': str(input_path),
            'output': str(output_path),
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio,
            'time_taken': end_time - start_time,
            'algorithm': 'bzip2'
        }
    except Exception as e:
        return f"Error compressing {file_path}: {str(e)}"

def compress_file_xz(file_path, output_dir=None, compression_level=6):
    """Compress a single file using xz/lzma"""
    try:
        input_path = Path(file_path)
        if not input_path.exists():
            return f"Error: {file_path} does not exist"
        
        if output_dir:
            output_path = Path(output_dir) / f"{input_path.name}.xz"
        else:
            output_path = input_path.with_suffix(input_path.suffix + '.xz')
        
        start_time = time.time()
        
        with open(input_path, 'rb') as f_in:
            with lzma.open(output_path, 'wb', preset=compression_level) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        end_time = time.time()
        original_size = input_path.stat().st_size
        compressed_size = output_path.stat().st_size
        compression_ratio = (1 - compressed_size / original_size) * 100
        
        return {
            'file': str(input_path),
            'output': str(output_path),
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio,
            'time_taken': end_time - start_time,
            'algorithm': 'xz'
        }
    except Exception as e:
        return f"Error compressing {file_path}: {str(e)}"

def get_compression_function(algorithm):
    """Return the appropriate compression function based on algorithm"""
    algorithms = {
        'gzip': compress_file_gzip,
        'bzip2': compress_file_bzip2,
        'xz': compress_file_xz
    }
    return algorithms.get(algorithm.lower(), compress_file_gzip)

def compress_files_parallel(file_list, algorithm='gzip', output_dir=None, 
                          compression_level=None, max_workers=None):
    """Compress multiple files in parallel using ProcessPoolExecutor"""
    
    if max_workers is None:
        max_workers = min(cpu_count(), len(file_list))
    
    compress_func = get_compression_function(algorithm)
    
    # Set default compression levels
    if compression_level is None:
        compression_level = 6 if algorithm in ['gzip', 'xz'] else 9
    
    print(f"Starting parallel compression of {len(file_list)} files")
    print(f"Algorithm: {algorithm.upper()}")
    print(f"Workers: {max_workers}")
    print(f"Compression Level: {compression_level}")
    print("-" * 50)
    
    start_time = time.time()
    results = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all compression tasks
        future_to_file = {
            executor.submit(compress_func, file_path, output_dir, compression_level): file_path 
            for file_path in file_list
        }
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
                
                if isinstance(result, dict):
                    print(f"✓ {Path(result['file']).name} -> {Path(result['output']).name}")
                    print(f"  Size: {result['original_size']:,} -> {result['compressed_size']:,} bytes")
                    print(f"  Ratio: {result['compression_ratio']:.1f}% | Time: {result['time_taken']:.2f}s")
                else:
                    print(f"✗ {result}")
                    
            except Exception as e:
                error_msg = f"Error processing {file_path}: {str(e)}"
                print(f"✗ {error_msg}")
                results.append(error_msg)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Print summary
    successful_results = [r for r in results if isinstance(r, dict)]
    if successful_results:
        total_original = sum(r['original_size'] for r in successful_results)
        total_compressed = sum(r['compressed_size'] for r in successful_results)
        avg_compression = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
        
        print("\n" + "=" * 50)
        print("COMPRESSION SUMMARY")
        print("=" * 50)
        print(f"Files processed: {len(successful_results)}/{len(file_list)}")
        print(f"Total original size: {total_original:,} bytes")
        print(f"Total compressed size: {total_compressed:,} bytes")
        print(f"Average compression ratio: {avg_compression:.1f}%")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Average time per file: {total_time/len(file_list):.2f} seconds")
    
    return results

def create_sample_files(directory="sample_files", num_files=5, file_size_kb=100):
    """Create sample files for testing compression"""
    sample_dir = Path(directory)
    sample_dir.mkdir(exist_ok=True)
    
    print(f"Creating {num_files} sample files in {directory}/")
    
    # Sample content patterns
    patterns = [
        "This is a sample text file with repeated content. " * 100,
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 80,
        "The quick brown fox jumps over the lazy dog. " * 120,
        "Python is a powerful programming language for data processing. " * 90,
        "Compression algorithms reduce file size by removing redundancy. " * 85
    ]
    
    created_files = []
    for i in range(num_files):
        file_path = sample_dir / f"sample_{i+1}.txt"
        content = patterns[i % len(patterns)]
        
        # Adjust content to approximate target size
        target_size = file_size_kb * 1024
        content = (content * (target_size // len(content) + 1))[:target_size]
        
        with open(file_path, 'w') as f:
            f.write(content)
        
        created_files.append(str(file_path))
        print(f"Created: {file_path} ({len(content):,} bytes)")
    
    return created_files

def main():
    parser = argparse.ArgumentParser(description='Parallel File Compression Tool')
    parser.add_argument('files', nargs='*', help='Files to compress')
    parser.add_argument('-a', '--algorithm', choices=['gzip', 'bzip2', 'xz'], 
                       default='gzip', help='Compression algorithm')
    parser.add_argument('-o', '--output-dir', help='Output directory')
    parser.add_argument('-l', '--level', type=int, help='Compression level')
    parser.add_argument('-w', '--workers', type=int, help='Number of worker processes')
    parser.add_argument('--create-samples', action='store_true', 
                       help='Create sample files for testing')
    parser.add_argument('--sample-count', type=int, default=5, 
                       help='Number of sample files to create')
    parser.add_argument('--sample-size', type=int, default=100, 
                       help='Size of sample files in KB')
    
    args = parser.parse_args()
    
    if args.create_samples:
        sample_files = create_sample_files(
            num_files=args.sample_count, 
            file_size_kb=args.sample_size
        )
        print(f"\nSample files created. You can now compress them with:")
        print(f"python3 {sys.argv[0]} {' '.join(sample_files)} -a {args.algorithm}")
        return
    
    if not args.files:
        print("No files specified. Use --create-samples to create test files.")
        print("Usage: python3 parallel_compress_python.py file1.txt file2.txt ...")
        return
    
    # Validate files exist
    valid_files = []
    for file_path in args.files:
        if Path(file_path).exists():
            valid_files.append(file_path)
        else:
            print(f"Warning: {file_path} does not exist, skipping...")
    
    if not valid_files:
        print("No valid files found to compress.")
        return
    
    # Create output directory if specified
    if args.output_dir:
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    # Run parallel compression
    results = compress_files_parallel(
        valid_files,
        algorithm=args.algorithm,
        output_dir=args.output_dir,
        compression_level=args.level,
        max_workers=args.workers
    )

if __name__ == "__main__":
    main()
