out=./tmp/out.txt
echo "" > $out

# times=10
# ./go-test-many.sh $times 8 TestSnapshotBasic3D >> $out

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
# go test -run 3B -race -v >> $out
# time go test -run 3B -v


# TestFigure83C
# TestFigure8Unreliable3C
# TestReliableChurn3C
# TestUnreliableChurn3C
# go test -run=TestPersist13C -race -v > $out
# go test -run=TestMyFigure83C -race -v > $out
# go test -run=TestFigure83C -race -v > $out
# go test -run=TestUnreliableAgree3C -race -v > $out
# go test -run=TestFigure8Unreliable3C -race -v > $out
# go test -run=TestMyFigure8Unreliable3C -race -v > $out
# go test -run 3C -race -v
# time go test -run 3C -v


# timeout -k 2s 20s go test -run=TestSnapshotBasic3D -race -v >> $out
# timeout -k 2s 120s go test -run=TestSnapshotInstall3D -race -v >> $out
# timeout -k 2s 120s go test -run=TestSnapshotInstallUnreliable3D -race -v >> $out

timeout -k 2s 600s go test -run 3D -race -v >> $out

# for i in {0..2}
# do
# go test -run 3B -race -v >> $out


# go test -run 3C -race -v >> $out

# # echo "hello, linux!"
# done


# go test -run=TestQA1 -race -v > $out

