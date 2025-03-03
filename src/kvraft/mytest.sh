out=./out/out.log
echo "" > $out

# go test -run=TestBasic4A -race -v >> $out
# go test -run=TestSnapshotRPC4B -race -v >> $out
# go test -run=TestSnapshotSize4B -race -v >> $out
# go test -run=TestSnapshotRecover4B -race -v >> $out
# go test -run=TestSnapshotRecoverManyClients4B -race -v >> $out
# go test -run=TestSnapshotUnreliable4B -race -v >> $out

# go test -run 4A -race -v >> $out
# go test -run 4B -race -v >> $out
# timeout -k 2s 100s go test -run=TestManyPartitionsOneClient4A -race -v >> $out
# TestManyPartitionsOneClient4A
# TestManyPartitionsManyClients4A
# TestPersistPartition4A
# TestPersistOneClient4A
# TestPersistConcurrent4A
# TestPersistConcurrentUnreliable4A
# TestPersistPartitionUnreliable4A
# TestPersistPartitionUnreliableLinearizable4A

# go test -run=TestSpeed4A -race -v >> $out
# go test -run=TestConcurrent4A -race -v >> $out

# ~/dstest -n 20 -p 10 -o ./out/ 4A 4B

# go test -run=TestSpeed4A -race -v >> $out
# go test -run=TestSpeed4B -race -v >> $out

# go test -race -v >> $out
