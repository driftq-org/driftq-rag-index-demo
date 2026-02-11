.PHONY: up down logs demo demo-clean ensure-env

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ifeq ($(OS),Windows_NT)
ensure-env:
	@if not exist .env ( \
		if exist .env.example ( \
			echo [make] .env missing; creating from .env.example && copy /Y .env.example .env >NUL \
		) else ( \
			echo [make] ERROR: .env missing and .env.example not found && exit /b 1 \
		) \
	)
else
ensure-env:
	@if [ ! -f .env ]; then \
		if [ -f .env.example ]; then \
			echo "[make] .env missing; creating from .env.example"; \
			cp .env.example .env; \
		else \
			echo "[make] ERROR: .env missing and .env.example not found"; \
			exit 1; \
		fi; \
	fi
endif

demo: ensure-env
	docker compose up --build --abort-on-container-exit --exit-code-from demo demo

demo-clean: ensure-env
	docker compose up --build --abort-on-container-exit --exit-code-from demo demo
	docker compose down -v
