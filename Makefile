.PHONY: install test lint check compose-up compose-down e2e logs clean

install:
	python3 -m pip install -e .[dev]

test:
	python3 -m pytest

lint:
	python3 -m ruff check .

check: lint test

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down

e2e:
	./scripts/e2e.sh

logs:
	docker compose logs -f --tail=100

clean:
	docker compose down -v
