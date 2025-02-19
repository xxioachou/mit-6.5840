out=./tmp/out.txt

# go test -run=TestInitialElection3A -race -v
# timeout -k 2s 16s go test -run=TestInitialElection3A -race -v > $out
# timeout -k 2s 16s go test -run=TestInitialElection3A -race -v 

# go test -run=TestReElection3A -race -v
# go test -run=TestReElection3A -race -v > $out


# go test -run=TestManyElections3A -race -v > $out

go test -run 3A -race -v
