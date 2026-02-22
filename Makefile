.PHONY: build up down restart logs shell mysql redis clean

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

shell:
	docker compose exec app bash

mysql:
	docker compose exec mysql mysql -u engine -pengine_pass engine_db

redis:
	docker compose exec redis redis-cli

clean:
	docker compose down -v --rmi local

status:
	docker compose ps
