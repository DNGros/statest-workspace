#!/bin/bash
# Process all states with 1 mile threshold
# Usage: ./process_all_states_1mi.sh

set -e

# Configuration
THRESHOLD_KM=1.609  # 1 mile in kilometers
INPUT_DIR="data/osm"
OUTPUT_DIR="data/streetdfs_1mi"
BINARY="./osm_processor_rust/target/release/osm_processor_rust"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Get list of all state OSM files
STATES=$(ls "$INPUT_DIR"/*.osm.pbf | xargs -n1 basename | sed 's/-latest.osm.pbf//' | sort)

echo "Processing all states with 1 mile (${THRESHOLD_KM} km) threshold..."
echo "Output directory: $OUTPUT_DIR"
echo "Total states: $(echo "$STATES" | wc -l)"
echo ""

# Process each state
for state in $STATES; do
    echo "=========================================="
    echo "Processing: $state"
    echo "=========================================="
    
    INPUT_FILE="$INPUT_DIR/${state}-latest.osm.pbf"
    OUTPUT_FILE="$OUTPUT_DIR/${state}_streets.parquet"
    
    if [ ! -f "$INPUT_FILE" ]; then
        echo "Warning: Input file not found: $INPUT_FILE"
        continue
    fi
    
    # Run the processor
    $BINARY "$state" "$INPUT_FILE" "$THRESHOLD_KM" "$OUTPUT_FILE" || {
        echo "Error processing $state"
        exit 1
    }
    
    echo ""
done

echo "=========================================="
echo "All states processed successfully!"
echo "Output directory: $OUTPUT_DIR"
echo "=========================================="

