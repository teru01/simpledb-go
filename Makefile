test:
	gotestsum -- -race ./...
build:
	go build -o simpledb ./
