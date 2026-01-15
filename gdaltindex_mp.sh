#!/bin/bash
#
# gdaltindex_mp.sh
# Process large lists of geospatial files using gdaltindex in parallel batches,
# then merge results into a single FlatGeoBuf file.
#

set -euo pipefail

# Default values
BATCH_SIZE=1000
NUM_CORES=$(nproc)
TEMP_DIR=""
CLEANUP=true

usage() {
    cat << EOF
Usage: $(basename "$0") -i INPUT_FILE -o OUTPUT_FILE [OPTIONS]

Process a list of geospatial files using gdaltindex in parallel batches,
then merge into a single FlatGeoBuf file.

Required arguments:
    -i, --input FILE       Text file containing list of geospatial files (one per line)
    -o, --output FILE      Output FlatGeoBuf file name (e.g., output.fgb)

Optional arguments:
    -b, --batch-size N     Number of files per batch (default: $BATCH_SIZE)
    -j, --jobs N           Number of parallel jobs (default: all CPUs = $NUM_CORES)
    -t, --temp-dir DIR     Directory for temporary files (default: auto-created)
    -k, --keep-temp        Keep temporary files after completion
    -h, --help             Show this help message

Example:
    $(basename "$0") -i file_list.txt -o index.fgb -b 500 -j 8
EOF
    exit "${1:-0}"
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

cleanup() {
    if [[ "$CLEANUP" == true && -n "$TEMP_DIR" && -d "$TEMP_DIR" ]]; then
        log "Cleaning up temporary directory: $TEMP_DIR"
        rm -rf "$TEMP_DIR"
    fi
}

# Parse arguments
INPUT_FILE=""
OUTPUT_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--input)
            INPUT_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -b|--batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -j|--jobs)
            NUM_CORES="$2"
            shift 2
            ;;
        -t|--temp-dir)
            TEMP_DIR="$2"
            shift 2
            ;;
        -k|--keep-temp)
            CLEANUP=false
            shift
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            error "Unknown option: $1"
            usage 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$INPUT_FILE" ]]; then
    error "Input file is required (-i)"
    usage 1
fi

if [[ -z "$OUTPUT_FILE" ]]; then
    error "Output file is required (-o)"
    usage 1
fi

if [[ ! -f "$INPUT_FILE" ]]; then
    error "Input file does not exist: $INPUT_FILE"
    exit 1
fi

# Validate numeric arguments
if ! [[ "$BATCH_SIZE" =~ ^[0-9]+$ ]] || [[ "$BATCH_SIZE" -lt 1 ]]; then
    error "Batch size must be a positive integer"
    exit 1
fi

if ! [[ "$NUM_CORES" =~ ^[0-9]+$ ]] || [[ "$NUM_CORES" -lt 1 ]]; then
    error "Number of jobs must be a positive integer"
    exit 1
fi

# Check for required tools
for cmd in gdaltindex ogrmerge.py; do
    if ! command -v "$cmd" &> /dev/null; then
        error "Required command not found: $cmd"
        exit 1
    fi
done

# Create temporary directory if not specified
if [[ -z "$TEMP_DIR" ]]; then
    TEMP_DIR=$(mktemp -d -t gdaltindex_mp.XXXXXX)
fi
mkdir -p "$TEMP_DIR"

# Set up cleanup trap
trap cleanup EXIT

log "Starting gdaltindex parallel processing"
log "Input file: $INPUT_FILE"
log "Output file: $OUTPUT_FILE"
log "Batch size: $BATCH_SIZE"
log "Parallel jobs: $NUM_CORES"
log "Temp directory: $TEMP_DIR"

# Count total files
TOTAL_FILES=$(wc -l < "$INPUT_FILE")
log "Total files to process: $TOTAL_FILES"

if [[ "$TOTAL_FILES" -eq 0 ]]; then
    error "Input file is empty"
    exit 1
fi

# Calculate number of batches
NUM_BATCHES=$(( (TOTAL_FILES + BATCH_SIZE - 1) / BATCH_SIZE ))
log "Number of batches: $NUM_BATCHES"

# Split input file into batches
log "Splitting input file into batches..."
BATCH_DIR="$TEMP_DIR/batches"
mkdir -p "$BATCH_DIR"
split -l "$BATCH_SIZE" -d -a 6 "$INPUT_FILE" "$BATCH_DIR/batch_"

# Directory for output geopackages
GPKG_DIR="$TEMP_DIR/gpkg"
mkdir -p "$GPKG_DIR"

# Function to process a single batch
process_batch() {
    local batch_file="$1"
    local batch_name
    batch_name=$(basename "$batch_file")
    local output_gpkg="$GPKG_DIR/${batch_name}.gpkg"

    # Run gdaltindex with the list of files from the batch
    if gdaltindex -f GPKG "$output_gpkg" --optfile "$batch_file" 2>/dev/null; then
        echo "Completed: $batch_name"
    else
        echo "Failed: $batch_name" >&2
        return 1
    fi
}

export -f process_batch
export GPKG_DIR

# Process batches in parallel
log "Processing $NUM_BATCHES batches with $NUM_CORES parallel jobs..."

# Use find + xargs for parallel processing (more portable than GNU parallel)
find "$BATCH_DIR" -type f -name 'batch_*' | sort | \
    xargs -P "$NUM_CORES" -I {} bash -c 'process_batch "$@"' _ {}

# Check if any geopackages were created
GPKG_COUNT=$(find "$GPKG_DIR" -name '*.gpkg' -type f | wc -l)
if [[ "$GPKG_COUNT" -eq 0 ]]; then
    error "No geopackages were created. Check if input files exist and are valid."
    exit 1
fi

log "Created $GPKG_COUNT geopackages"

# Merge all geopackages into final FlatGeoBuf
log "Merging geopackages into final FlatGeoBuf file..."

# Ensure output directory exists
OUTPUT_DIR=$(dirname "$OUTPUT_FILE")
if [[ -n "$OUTPUT_DIR" && "$OUTPUT_DIR" != "." ]]; then
    mkdir -p "$OUTPUT_DIR"
fi

# Remove output file if it exists (ogrmerge won't overwrite)
if [[ -f "$OUTPUT_FILE" ]]; then
    rm -f "$OUTPUT_FILE"
fi

ogrmerge.py -progress -single -o "$OUTPUT_FILE" -f FlatGeobuf "$GPKG_DIR"/*.gpkg

if [[ -f "$OUTPUT_FILE" ]]; then
    log "Successfully created: $OUTPUT_FILE"
    OUTPUT_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    log "Output file size: $OUTPUT_SIZE"
else
    error "Failed to create output file"
    exit 1
fi

log "Processing complete!"
