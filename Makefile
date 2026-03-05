test:
	gotestsum -- -race ./...
build:
	go build -o simpledb ./
build-bench:
	go build -o benchmark ./dbcmd/benchmark
