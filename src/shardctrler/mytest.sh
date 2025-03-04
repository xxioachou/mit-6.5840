out=./logs/out.log
echo "" > $out

# go test -run=TestBasic -race -v >> $out
# go test -run=TestMulti -race -v >> $out

go test -race -v >> $out
