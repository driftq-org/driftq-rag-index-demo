.PHONY: up down logs demo demo-clean

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

demo:
	docker compose up --build --abort-on-container-exit --exit-code-from demo demo

demo-clean:
	docker compose up --build --abort-on-container-exit --exit-code-from demo demo
	docker compose down -v
