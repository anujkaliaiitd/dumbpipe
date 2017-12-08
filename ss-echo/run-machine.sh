#!/usr/bin/env bash
source $(dirname $0)/../scripts/utils.sh
source $(dirname $0)/../scripts/mlx_env.sh
source $(dirname $0)/params.sh
export HRD_REGISTRY_IP="specialnode.RDMA.fawn.apt.emulab.net"

drop_shm

# lsync messes up permissions
executable="../build/ss-echo"
chmod +x $executable

blue "Running $num_client_threads client threads"

# Check number of arguments
if [ "$#" -gt 2 ]; then
  blue "Illegal number of arguments."
  blue "Usage: ./run-machine.sh <machine_id>, or ./run-machine.sh <machine_id> gdb"
	exit
fi

if [ "$#" -eq 0 ]; then
  blue "Illegal number of arguments."
  blue "Usage: ./run-machine.sh <machine_id>, or ./run-machine.sh <machine_id> gdb"
	exit
fi

flags="\
  --num_client_threads $num_client_threads \
  --num_server_threads $num_server_threads \
	--dual_port $dual_port \
  --postlist $postlist \
	--is_client 1 \
  --size $size \
	--machine_id $1
"

# Check for non-gdb mode
if [ "$#" -eq 1 ]; then
  sudo -E numactl --cpunodebind=0 --membind=0 $executable $flags
fi

# Check for gdb mode
if [ "$#" -eq 2 ]; then
  sudo -E gdb -ex run --args $executable $flags
fi
