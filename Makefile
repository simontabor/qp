RUNNER ?= ./node_modules/mocha/bin/mocha

run-test = $(RUNNER) $(1)

.SILENT:
.PHONY: test

test:
	$(call run-test,./test/*.js)
