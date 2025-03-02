out=./out/out.log
echo "" > $out

# go test -run=TestBasic4A -race -v >> $out

go test -run 4A -race -v >> $out
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
