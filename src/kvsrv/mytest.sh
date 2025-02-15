go test -run=TestBasic2 -race

go test -run=TestConcurrent2 -race

go test -run=TestUnreliable2 -race

go test -run=TestUnreliableOneKey2 -race

go test -run=TestMemGet2 -race

go test -run=TestMemPut2 -race

go test -run=TestMemPutManyClients -race