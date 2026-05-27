.PHONY: start transfer logs stop clean status

start:
	docker compose up -d
	@echo ""
	@echo "Сервисы запускаются (первый раз ~3-5 минут)..."
	@echo ""
	@echo "  PostgreSQL : localhost:5432    (etl_user / etl_pass / source_db)"
	@echo "  MinIO      : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Airflow    : http://localhost:8080  (admin / admin)"

transfer:
	docker compose --profile transfer run --rm transfer

logs:
	docker compose logs -f airflow-webserver airflow-scheduler

status:
	docker compose ps

stop:
	docker compose down

clean:
	docker compose down -v
	@echo "Все данные удалены."
