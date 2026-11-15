.PHONY: install backend frontend migrate-init migrate migrate-resume migrate-status migrate-verify migrate-reset test

install:
	cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
	cd frontend && npm install

backend:
	cd backend && ./run.sh

frontend:
	cd frontend && npm run dev

# ===== 产品管线运行状态迁移（本地开发 / 构建产物 / 上线部署通用，均为幂等命令）=====
migrate-init:
	cd backend && .venv/bin/python -m app.persistence.cli init

migrate:
	cd backend && .venv/bin/python -m app.persistence.cli init
	cd backend && .venv/bin/python -m app.persistence.cli migrate

migrate-resume:
	cd backend && .venv/bin/python -m app.persistence.cli migrate

migrate-status:
	cd backend && .venv/bin/python -m app.persistence.cli status

migrate-verify:
	cd backend && .venv/bin/python -m app.persistence.cli verify

migrate-reset:
	cd backend && .venv/bin/python -m app.persistence.cli reset

test:
	cd backend && python3 -m unittest discover -s tests -v
