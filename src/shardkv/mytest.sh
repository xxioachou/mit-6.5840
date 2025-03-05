out=./logs/out.log
echo "" > $out

# go test -run=TestStaticShards5A -race -v >> $out
# go test -run=TestRejection5A -race -v >> $out

# go test -run 5A -race -v >> $out
# go test -run=TestJoinLeave5B -race -v >> $out
go test -run=TestSnapshot5B -race -v >> $out
# go test -run 5B -race -v >> $out
