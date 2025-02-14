rm -rf mr-*
mkdir mr-tmp-my
cd mr-tmp-my

RACE=-race

go build $RACE -buildmode=plugin ../../mrapps/nocrash.go
go build $RACE -buildmode=plugin ../../mrapps/crash.go
go build $RACE ../mrsequential.go 
go build $RACE ../mrcoordinator.go
go build $RACE ../mrworker.go

./mrsequential nocrash.so ../my-test*.txt
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*
cat mr-correct-crash.txt
echo "-------------------------------"

# go build -buildmode=plugin ../mrapps/early_exit.go

# coordinator
# go run mrcoordinator.go pg-*.txt

# worker
# go run mrworker.go early_exit.so

./mrcoordinator ../my-test*.txt;
sort mr-out-* | grep . > mr-crash-all
cat mr-crash-all
