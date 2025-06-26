#!/bin/bash

# Parallel File Compression using Shell Script
# Supports gzip, bzip2, xz compression with GNU parallel or background processes

set -euo pipefail

# Default values
ALGORITHM="gzip"
OUTPUT_DIR=""
COMPRESSION_LEVEL=""
MAX_JOBS=""
VERBOSE=false
USE_GNU_PARALLEL=true

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] FILE1 [FILE2 ...]

Parallel File Compression Tool

OPTIONS:
    -a, --algorithm ALGO    Compression algorithm: gzip, bzip2, xz (default: gzip)
    -o, --output-dir DIR    Output directory for compressed files
    -l, --level LEVEL       Compression level (1-9 for gzip/bzip2, 0-9 for xz)
    -j, --jobs NUM          Maximum number of parallel jobs (default: CPU cores)
    -v, --verbose           Verbose output
    -p, --use-parallel      Use GNU parallel if available (default: true)
    -b, --background        Use background processes instead of GNU parallel
    -h, --help              Show this help message
    --create-samples        Create sample files for testing
    --sample-count NUM      Number of sample files to create (default: 5)
    --sample-size KB        Size of each sample file in KB (default: 100)

EXAMPLES:
    $0 file1.txt file2.txt file3.txt
    $0 -a bzip2 -l 9 -o compressed/ *.txt
    $0 --create-samples --sample-count 10
    $0 -a xz -j 4 sample_files/*.txt

EOF
}

# Function to check if GNU parallel is available
check_gnu_parallel() {
    if command -v parallel >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Function to get number of CPU cores
get_cpu_cores() {
    if command -v nproc >/dev/null 2>&1; then
        nproc
    elif [ -f /proc/cpuinfo ]; then
        grep -c ^processor /proc/cpuinfo
    else
        echo "4"  # fallback
    fi
}

# Function to compress a single file
compress_single_file() {
    local input_file="$1"
    local algorithm="$2"
    local output_dir="$3"
    local compression_level="$4"
    
    if [ ! -f "$input_file" ]; then
        print_error "File not found: $input_file"
        return 1
    fi
    
    local filename=$(basename "$input_file")
    local output_file
    
    if [ -n "$output_dir" ]; then
        mkdir -p "$output_dir"
        case "$algorithm" in
            gzip)   output_file="$output_dir/${filename}.gz" ;;
            bzip2)  output_file="$output_dir/${filename}.bz2" ;;
            xz)     output_file="$output_dir/${filename}.xz" ;;
        esac
    else
        case "$algorithm" in
            gzip)   output_file="${input_file}.gz" ;;
            bzip2)  output_file="${input_file}.bz2" ;;
            xz)     output_file="${input_file}.xz" ;;
        esac
    fi
    
    local start_time=$(date +%s.%N)
    local original_size=$(stat -c%s "$input_file")
    
    case "$algorithm" in
        gzip)
            if [ -n "$compression_level" ]; then
                gzip -c -"$compression_level" "$input_file" > "$output_file"
            else
                gzip -c "$input_file" > "$output_file"
            fi
            ;;
        bzip2)
            if [ -n "$compression_level" ]; then
                bzip2 -c -"$compression_level" "$input_file" > "$output_file"
            else
                bzip2 -c "$input_file" > "$output_file"
            fi
            ;;
        xz)
            if [ -n "$compression_level" ]; then
                xz -c -"$compression_level" "$input_file" > "$output_file"
            else
                xz -c "$input_file" > "$output_file"
            fi
            ;;
    esac
    
    local end_time=$(date +%s.%N)
    local compressed_size=$(stat -c%s "$output_file")
    local time_taken=$(echo "$end_time - $start_time" | bc -l)
    local compression_ratio=$(echo "scale=1; (1 - $compressed_size / $original_size) * 100" | bc -l)
    
    printf "✓ %s -> %s\n" "$(basename "$input_file")" "$(basename "$output_file")"
    printf "  Size: %'d -> %'d bytes | Ratio: %.1f%% | Time: %.2fs\n" \
           "$original_size" "$compressed_size" "$compression_ratio" "$time_taken"
    
    return 0
}

# Function to compress files using GNU parallel
compress_with_gnu_parallel() {
    local files=("$@")
    
    print_info "Using GNU parallel for compression"
    print_info "Algorithm: ${ALGORITHM^^}"
    print_info "Files: ${#files[@]}"
    print_info "Max jobs: $MAX_JOBS"
    
    export -f compress_single_file print_error
    export ALGORITHM OUTPUT_DIR COMPRESSION_LEVEL
    
    printf '%s\n' "${files[@]}" | \
    parallel -j "$MAX_JOBS" --bar compress_single_file {} "$ALGORITHM" "$OUTPUT_DIR" "$COMPRESSION_LEVEL"
}

# Function to compress files using background processes
compress_with_background() {
    local files=("$@")
    local pids=()
    local active_jobs=0
    
    print_info "Using background processes for compression"
    print_info "Algorithm: ${ALGORITHM^^}"
    print_info "Files: ${#files[@]}"
    print_info "Max jobs: $MAX_JOBS"
    
    for file in "${files[@]}"; do
        # Wait if we've reached max jobs
        while [ "$active_jobs" -ge "$MAX_JOBS" ]; do
            for i in "${!pids[@]}"; do
                if ! kill -0 "${pids[$i]}" 2>/dev/null; then
                    wait "${pids[$i]}"
                    unset "pids[$i]"
                    ((active_jobs--))
                fi
            done
            sleep 0.1
        done
        
        # Start compression in background
        compress_single_file "$file" "$ALGORITHM" "$OUTPUT_DIR" "$COMPRESSION_LEVEL" &
        pids+=($!)
        ((active_jobs++))
        
        if [ "$VERBOSE" = true ]; then
            print_info "Started compression of $(basename "$file") (PID: $!)"
        fi
    done
    
    # Wait for all background jobs to complete
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            wait "$pid"
        fi
    done
}

# Function to create sample files
create_sample_files() {
    local sample_count=${1:-5}
    local sample_size_kb=${2:-100}
    local sample_dir="sample_files"
    
    mkdir -p "$sample_dir"
    print_info "Creating $sample_count sample files in $sample_dir/"
    
    local patterns=(
        "This is a sample text file with repeated content. "
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
        "The quick brown fox jumps over the lazy dog. "
        "Shell scripting is powerful for system administration tasks. "
        "Parallel processing improves performance significantly. "
    )
    
    for ((i=1; i<=sample_count; i++)); do
        local file_path="$sample_dir/sample_$i.txt"
        local pattern_index=$(((i-1) % ${#patterns[@]}))
        local pattern="${patterns[$pattern_index]}"
        
        # Create content to approximate target size
        local target_size=$((sample_size_kb * 1024))
        local content=""
        
        while [ ${#content} -lt $target_size ]; do
            content="$content$pattern"
        done
        
        # Trim to exact size
        content="${content:0:$target_size}"
        
        echo "$content" > "$file_path"
        local actual_size=$(stat -c%s "$file_path")
        print_success "Created: $file_path ($actual_size bytes)"
    done
    
    print_info "Sample files created. You can compress them with:"
    print_info "$0 -a $ALGORITHM $sample_dir/*.txt"
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -a|--algorithm)
                ALGORITHM="$2"
                if [[ ! "$ALGORITHM" =~ ^(gzip|bzip2|xz)$ ]]; then
                    print_error "Invalid algorithm: $ALGORITHM"
                    exit 1
                fi
                shift 2
                ;;
            -o|--output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            -l|--level)
                COMPRESSION_LEVEL="$2"
                if [[ ! "$COMPRESSION_LEVEL" =~ ^[0-9]$ ]]; then
                    print_error "Invalid compression level: $COMPRESSION_LEVEL"
                    exit 1
                fi
                shift 2
                ;;
            -j|--jobs)
                MAX_JOBS="$2"
                if [[ ! "$MAX_JOBS" =~ ^[0-9]+$ ]]; then
                    print_error "Invalid number of jobs: $MAX_JOBS"
                    exit 1
                fi
                shift 2
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -p|--use-parallel)
                USE_GNU_PARALLEL=true
                shift
                ;;
            -b|--background)
                USE_GNU_PARALLEL=false
                shift
                ;;
            --create-samples)
                CREATE_SAMPLES=true
                shift
                ;;
            --sample-count)
                SAMPLE_COUNT="$2"
                shift 2
                ;;
            --sample-size)
                SAMPLE_SIZE="$2"
                shift 2
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            -*)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                FILES+=("$1")
                shift
                ;;
        esac
    done
}

# Main function
main() {
    local FILES=()
    local CREATE_SAMPLES=false
    local SAMPLE_COUNT=5
    local SAMPLE_SIZE=100
    
    # Parse arguments
    parse_arguments "$@"
    
    # Set default max jobs if not specified
    if [ -z "$MAX_JOBS" ]; then
        MAX_JOBS=$(get_cpu_cores)
    fi
    
    # Handle sample creation
    if [ "$CREATE_SAMPLES" = true ]; then
        create_sample_files "$SAMPLE_COUNT" "$SAMPLE_SIZE"
        return 0
    fi
    
    # Check if files were provided
    if [ ${#FILES[@]} -eq 0 ]; then
        print_error "No files specified"
        show_usage
        exit 1
    fi
    
    # Validate files exist
    local valid_files=()
    for file in "${FILES[@]}"; do
        if [ -f "$file" ]; then
            valid_files+=("$file")
        else
            print_warning "File not found: $file"
        fi
    done
    
    if [ ${#valid_files[@]} -eq 0 ]; then
        print_error "No valid files found"
        exit 1
    fi
    
    # Check compression tools
    case "$ALGORITHM" in
        gzip)
            if ! command -v gzip >/dev/null 2>&1; then
                print_error "gzip not found"
                exit 1
            fi
            ;;
        bzip2)
            if ! command -v bzip2 >/dev/null 2>&1; then
                print_error "bzip2 not found"
                exit 1
            fi
            ;;
        xz)
            if ! command -v xz >/dev/null 2>&1; then
                print_error "xz not found"
                exit 1
            fi
            ;;
    esac
    
    # Start compression
    local start_time=$(date +%s.%N)
    
    if [ "$USE_GNU_PARALLEL" = true ] && check_gnu_parallel; then
        compress_with_gnu_parallel "${valid_files[@]}"
    else
        if [ "$USE_GNU_PARALLEL" = true ]; then
            print_warning "GNU parallel not found, falling back to background processes"
        fi
        compress_with_background "${valid_files[@]}"
    fi
    
    local end_time=$(date +%s.%N)
    local total_time=$(echo "$end_time - $start_time" | bc -l)
    
    echo
    print_success "Compression completed!"
    printf "Total time: %.2f seconds\n" "$total_time"
    printf "Files processed: %d\n" "${#valid_files[@]}"
    printf "Average time per file: %.2f seconds\n" "$(echo "$total_time / ${#valid_files[@]}" | bc -l)"
}

# Run main function with all arguments
main "$@"
