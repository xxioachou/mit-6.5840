#!/usr/bin/env bash

# $# 表示传递给脚本的参数个数
# $0 是脚本的名称
if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

# trap命令用于捕获信号并执行指定的命令，捕获 INT 信号(ctrl + c)
trap 'kill -INT -$pid; exit 1' INT

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
# $1 是第一个参数的内容
runs=$1
chmod +x test-mr.sh

# 执行 $runs 次
for i in $(seq 1 $runs); do
    # & 表示后台运行
    timeout -k 2s 900s ./test-mr.sh &
    # $!: 后台运行的进程 ID
    pid=$!
    # 执行失败（返回了非 0 值）
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS
