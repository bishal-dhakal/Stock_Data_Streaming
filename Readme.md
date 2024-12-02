Manually add partations on the topics

Run Producer as a module:
python -m kafka.producers.py

make migrations
alembic revision --autogenerate -m "Initial migration"

migrate
alembic upgrade head
