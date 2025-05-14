#!/usr/bin/env bash

iters=20

if [ $# -gt 0 ]; then
  iters="$1"
fi

cmd="cabal run . -- run examples/bench_parallel_sum_range.hvms -s"

# Initialize variables
total_mips=0
min_mips=999999999  # Start with a very high number
max_mips=0

echo "Running $cmd for $iters iterations..."

# Run the program N times
for (( i=1; i<=$iters; i++ )); do
    #echo -n "Run $i/$ITERATIONS: "
    
    output=$($cmd 2>/dev/null)
    
    mips=$(echo "$output" | grep "MIPS:" | awk '{print $2}')
    
    if [ -z "$mips" ]; then
        echo "ERROR: Could not find MIPS value in program output"
        continue
    fi
    
    if [ $mips -lt $min_mips ]; then
        min_mips=$mips
    fi
    
    if [ $mips -gt $max_mips ]; then
        max_mips=$mips
    fi
    
    total_mips=$((total_mips + mips))
done

avg_mips=$(echo "scale=2; $total_mips / $iters" | bc -l)

# Print summary to console
echo "--------"
echo "Minimum MIPS: $min_mips"
echo "Maximum MIPS: $max_mips"
echo "Average MIPS: $avg_mips"
