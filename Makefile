all: jikan

jikan: *.go **/*.go
	go build -o jikan

demo: jikan
	./jikan import demo/demo.db demo-stream demo/demo.csv
	./jikan export demo/demo.db demo-stream

clean:
	rm -f jikan demo/demo.db
