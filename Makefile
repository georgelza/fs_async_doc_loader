
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:


- run: start (create containers) for the entire environment
- run_app: run the FS application
- start: start the entire environment
- stop: stop the entire environment
- down: tear down the entire environment

- ps: show running containers
- logs: show logs
- logsf: show logs in real time
- watch: watch running containers

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

run:
	docker compose -p fi up -d mongodb


run_app:
	./runs.sh

stop:
	docker compose stop

start:
	docker compose start

down:	
	docker compose down


ps:
	docker compose ps

logs:
	docker compose logs

logsf:
	docker compose logs -f

watch:
	watch docker compose ps