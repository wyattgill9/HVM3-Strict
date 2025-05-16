#!/bin/bash

num_iters=20

if [ $# -gt 0 ]; then
  num_iters="$1"
fi

cmd="cabal run . -- run examples/bench_parallel_sum_range.hvms -s"

total_mips=0
min_mips=999999999
max_mips=0
first_itrs=0
first_size=0
outfile=out

echo "Running $cmd for $num_iters iterations..."

for (( i=1; i<=$num_iters; i++ )); do
    output=$($cmd 2>$outfile)
    
    mips=$(echo "$output" | grep "MIPS:" | awk '{print $2}')
    itrs=$(echo "$output" | grep "ITRS:" | awk '{print $2}')
    size=$(echo "$output" | grep "SIZE:" | awk '{print $2}')
    
    if [ -z "$mips" ] || [ -z "$itrs" ] || [ -z "$size" ]; then
        echo "ERROR: Could not find MIPS, ITRS, or SIZE in program output"
        echo "Output:"
        echo "$output"
        continue
    fi
    
    if [ "$first_itrs" -eq 0 ]; then
        first_itrs="$itrs"
    elif [ "$first_itrs" -ne "$itrs" ]; then
        echo "ERROR: ITRS: $itrs mismatch with first ITRS: $first_itrs"
        #echo "Output:"
        #echo "$output"
        #exit 1
    fi

    if [ "$first_size" -eq 0 ]; then
        first_size="$size"
    elif [ "$first_size" -ne "$size" ]; then
        echo "ERROR: SIZE: $size mismatch with first SIZE: $first_size"
        #exit 1
    fi

    if [ "$mips" -lt "$min_mips" ]; then
        min_mips="$mips"
    fi
    
    if [ "$mips" -gt "$max_mips" ]; then
        max_mips="$mips"
    fi
    
    total_mips=$((total_mips + mips))
done

avg_mips=$(echo "scale=2; $total_mips / $num_iters" | bc -l)

echo "--------"
echo "Minimum MIPS: $min_mips"
echo "Maximum MIPS: $max_mips"
echo "Average MIPS: $avg_mips"
