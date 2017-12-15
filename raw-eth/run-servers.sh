#!/usr/bin/env bash
source $(dirname $0)/../scripts/utils.sh
source $(dirname $0)/../scripts/mlx_env.sh
source $(dirname $0)/params.sh

executable="../build/raw-eth"
blue "Starting $num_server_threads server threads"

flags="
	--num_server_threads $num_server_threads \
	--dual_port $dual_port \
	--is_client 0 \
	--size $size \
	--postlist $postlist
"

# Check for non-gdb mode
if [ "$#" -eq 0 ]; then
  sudo -E numactl --cpunodebind=0 --membind=0 $executable $flags
fi

# Check for gdb mode
if [ "$#" -eq 1 ]; then
  sudo -E gdb -ex run --args $executable $flags
fi
