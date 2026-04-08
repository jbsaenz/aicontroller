.PHONY: install phase2 phase3 phase4 phase5 phase23 phase24 phase25 docker-build docker-run

install:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

phase2:
	. .venv/bin/activate && python src/pipeline.py --phase phase2

phase3:
	. .venv/bin/activate && python src/pipeline.py --phase phase3

phase4:
	. .venv/bin/activate && python src/pipeline.py --phase phase4

phase5:
	. .venv/bin/activate && python src/pipeline.py --phase phase5

phase23:
	. .venv/bin/activate && python src/pipeline.py --phase phase2-3

phase24:
	. .venv/bin/activate && python src/pipeline.py --phase phase2-4

phase25:
	. .venv/bin/activate && python src/pipeline.py --phase phase2-5

docker-build:
	docker build -t aicontroller:latest .

docker-run:
	docker run --rm -v "$(PWD):/app" aicontroller:latest
