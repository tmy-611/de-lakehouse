SHELL := /bin/bash
include .env
export

.PHONY: up down logs test lint compose-check

up:
	docker compose up -d --build

down: 
	docker compose down -v

logs:
	docker compose logs -f --tail=100

test:
	python -m -pytest -v

lint:
	ruff check .
	sqlfluff lint dbt/models --dialect progres

compose-check:
	docker compose config --quiet