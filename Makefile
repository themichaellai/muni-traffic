.PHONY: build clean lint lint-fix watch

build:
	./node_modules/.bin/tsc

clean:
	rm -r build

lint:
	./node_modules/.bin/tslint src/**/*

lint-fix:
	./node_modules/.bin/tslint --fix src/**/*

watch: clean
	./node_modules/.bin/tsc -w
