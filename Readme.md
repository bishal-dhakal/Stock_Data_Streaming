Manually add partations on the topics

Run Producer:
python kafka/producers.py

make migrations
alembic revision --autogenerate -m "Initial migration"

migrate
alembic upgrade head
