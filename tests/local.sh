#!/bin/bash
# set -x


pkill -9 caffe
pkill -9 test_connection

if [ $# -lt 3 ]; then
    echo "usage: $0 num_servers num_workers bin [args..]"
    exit -1;
fi

export DMLC_NUM_SERVER=$1
shift
export DMLC_NUM_WORKER=$1
shift
bin=$1
shift
arg="$@"

# start the scheduler
export DMLC_PS_ROOT_URI='127.0.0.1'
export DMLC_PS_ROOT_PORT=8000
export DMLC_ROLE='scheduler'
/home/zrji/distributed_caffe/ps-lite-sj/tests/test_connection &

# start servers
export DMLC_ROLE='server'
for ((i=0; i<1; ++i)); do
    export HEAPPROFILE=./S${i}
    /home/zrji/distributed_caffe/ps-lite-sj/tests/test_connection &
done

# start workers
export DMLC_ROLE='worker'
for ((i=0; i<5; ++i)); do
    export HEAPPROFILE=./W${i}
    /home/zrji/distributed_caffe/caffe_cpu_sj/build/tools/caffe train --solver=/home/zrji/distributed_caffe/caffe_cpu_sj/examples/mnist/lenet_solver.prototxt &    
done

wait
