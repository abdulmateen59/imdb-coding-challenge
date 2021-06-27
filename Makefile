build:
	docker-compose up -d --build

run:
	docker exec -t spark-master spark-submit runner.py --Remote True

run-testcases:
	docker exec -t spark-master pytest -s -v test

status:
	docker attach spark-master --sig-proxy=false

stop:
	docker-compose down