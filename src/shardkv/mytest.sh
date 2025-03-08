out=./logs/out.log
echo "" > $out

# go test -run=TestStaticShards5A -race -v >> $out
# go test -run=TestRejection5A -race -v >> $out
# go test -run=TestConcurrent3_5B -race -v >> $out

# go test -run 5A -race -v >> $out
# go test -run=TestJoinLeave5B -race -v >> $out
# go test -run=TestSnapshot5B -race -v >> $out
# go test -run=TestMissChange5B -race -v >> $out
# go test -run=TestConcurrent1_5B -race -v >> $out
# go test -run 5B -race -v >> $out
# go test -run=TestChallenge1Delete -race -v >> $out
# go test -run Challenge -race -v >> $out

# iters=10
# i=1
# while (( $i <= $iters))
# do

#     go test -race -v > $out
#     if [ $? -ne 0 ]; then
#         echo "Test failed on test $i!"
#         fail="./logs/fail_${i}.log"
#         cp $out $fail
#         exit 1  
#     fi
#     echo "Test $i passed."
#     let "i++"
# done

# echo "Test passed $iters times!"
