all: jikan

jikan: **/*.go
	go build -o jikan

demo: jikan
	dd if=/dev/zero of=demo/demo.db bs=1m count=1
	./jikan import demo/demo.db demo/demo.csv
	./jikan export demo/demo.db

clean:
	rm -f jikan demo/demo.db
