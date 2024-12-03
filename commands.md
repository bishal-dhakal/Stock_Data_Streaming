Manually add partations on the topics
/opt/kafka-3.9.0-src/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic stock_data --partitions 5

Read data on the partations
/opt/kafka-3.9.0-src/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic stock_data --from-beginning --partition 0 


Run Producer as a module:
python -m kafka.producers.py

make migrations
alembic revision --autogenerate -m "Initial migration"

migrate
alembic upgrade head