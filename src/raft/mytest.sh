out=./tmp/out.txt

# go test -run=TestInitialElection3A -race -v
# timeout -k 2s 16s go test -run=TestInitialElection3A -race -v > $out
# timeout -k 2s 16s go test -run=TestInitialElection3A -race -v 

# go test -run=TestReElection3A -race -v
# go test -run=TestReElection3A -race -v > $out


# go test -run=TestManyElections3A -race -v > $out

# go test -run 3A -race -v



# go test -run=TestBasicAgree3B -race -v > $out
# go test -run=TestRPCBytes3B -race -v > $out
# go test -run=TestFollowerFailure3B -race -v > $out
# go test -run=TestLeaderFailure3B -race -v > $out
# go test -run=TestFailAgree3B -race -v > $out
# go test -run=TestFailNoAgree3B -race -v > $out
# go test -run=TestRejoin3B -race -v > $out
# go test -run=TestMyBackup3B -race -v > $out
# go test -run 3B -race -v
time go test -run 3B -v

# i=0
# while (( $i < 10 )) 
# do
# go test -run=TestMyBackup3B -race -v >> $out

# i=`expr $i + 1`
# done



