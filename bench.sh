#!/usr/bin/env bash

num_itrs=20

if [[ $# > 0 ]]; then
  num_itrs="$1"
fi

cleanup() {
    echo "Caught CTRL+C, cleaning up..."
    pkill -P $$
    exit 1
}

trap cleanup SIGINT

cmd="cabal run . -- run examples/bench_parallel_sum_range.hvms -s"

tot_mips=0
min_mips=999999999
max_mips=0
fst_itrs=0
outfile=out.bench

echo "Running $cmd for $num_itrs iterations..."

for (( i=1; i<=$num_itrs; i++ )); do
    output=$($cmd 2>$outfile)
    
    mips=$(echo "$output" | grep "MIPS:" | awk '{print $2}')
    itrs=$(echo "$output" | grep "ITRS:" | awk '{print $2}')
    size=$(echo "$output" | grep "SIZE:" | awk '{print $2}')
    
    [[ -z "$itrs" ]] && err=1 || err=0

    if (( !err )) && [[ $fst_itrs == 0 ]]; then
        fst_itrs="$itrs"
    elif (( err )) || [[ $fst_itrs != $itrs ]]; then
        if (( err )); then
            echo "ERROR @ $i: missing ITRS"
        else
            echo "ERROR @ $i: ITRS: $itrs mismatch with first ITRS: $fst_itrs"
        fi
        echo "Output:"
        echo "$output"
        echo "------"
        echo "tail $outfile:"
        tail "$outfile"
        exit 1
    fi

    if [[ $mips < $min_mips ]]; then
        min_mips="$mips"
    fi
    
    if [[ $mips > $max_mips ]]; then
        max_mips="$mips"
    fi
    
    tot_mips=$((tot_mips + mips))
done

avg_mips=$(echo "scale=2; $tot_mips / $num_itrs" | bc -l)

echo "--------"
echo "Min MIPS: $min_mips"
echo "Max MIPS: $max_mips"
echo "Avg MIPS: $avg_mips"
