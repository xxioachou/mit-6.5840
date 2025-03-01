out=./tmp/out.txt
echo "" > $out

# times=1
# ./go-test-many.sh $times 8 >> $out

# mkdir test_errs
# mkdir test_logs
# mv *.err ./test_errs/
# mv *.log ./test_logs/

# go test -run=TestInitialElection3A -race -v >> $out
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


# timeout -k 2s 20s go test -rtime go test >> $outun=TestSnapshotBasic3D -race -v >> $out
# timeout -k 2s 120s go test -run=TestSnapshotInstall3D -race -v >> $out
# timeout -k 2s 120s go test -run=TestSnapshotInstallUnreliable3D -race -v >> $out
# timeout -k 2s 20s go test -run=TestSnapshotAllCrash3D -race -v >> $out
# timeout -k 2s 20s go test -run=TestSnapshotInit3D -race -v >> $out

# timeout -k 2s 600s go test -run 3D -race -v >> $out

# for i in {0..4}
# do

# go test -run=TestSnapshotInstallUnreliable3D -race -v >> $out

# done


# go test -run=TestQA1 -race -v > $out

rm ./tmp/*.log
# ./dstest -n 200 -p 10 -o ./tmp/ TestMyFigure8Unreliable3C TestFigure8Unreliable3C TestPersist23C
./dstest -n 600 -p 10 -o ./tmp/ 3A 3B 3C 3D
# ./dstest -n 200 -p 10 -o ./tmp/ TestPersist23C
# ./dstest -n 20 -p 10 -o ./tmp/ 3A
