all: main cli

cli: main cli/*.go
	go build -o jikan ./cli

main: *.go
	go build

demo: cli
	dd if=/dev/zero of=demo.db bs=1m count=1
	./jikan

clean:
	rm -f jikan demo.db
