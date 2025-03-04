out=./logs/out.log
echo "" > $out

# go test -run=TestStaticShards5A -race -v >> $out
go test -run=TestRejection5A -race -v >> $out
