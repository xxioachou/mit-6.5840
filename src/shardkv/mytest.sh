out=./logs/out.log
echo "" > $out

# go test -run=TestStaticShards5A -race -v >> $out
# go test -run=TestRejection5A -race -v >> $out

# go test -run 5A -race -v >> $out
# go test -run=TestJoinLeave5B -race -v >> $out
# go test -run=TestSnapshot5B -race -v >> $out
# go test -run=TestMissChange5B -race -v >> $out
# go test -run=TestConcurrent1_5B -race -v >> $out
go test -run 5B -race -v >> $out

# for i in {0..9}
# do

#     echo "" > $out
#     go test -run=TestConcurrent1_5B -race -v >> $out

#     if grep -w "FAIL" $out; then
#         echo "WA!"
#         exit 1
#     fi
# done
# echo "AC"
