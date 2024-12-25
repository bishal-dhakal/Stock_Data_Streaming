################kakfa####################
1. Manually add partations on the topics
    /opt/kafka-3.9.0-src/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic stock_data --partitions 5

2. Read data on the partations
    /opt/kafka-3.9.0-src/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic stock_data --from-beginning --partition 0

#########python module######################
3. Run Producer as a module:
    python -m kafka.producers.py

############alembic##########################
4. make migrations
    alembic revision --autogenerate -m "Initial migration"

5. migrate
    alembic upgrade head

#####################redis########################
6. Redis Command:
    6.1 key *                   --> list all the keys

    6.2 LRANGE stock:GOOGL 0 -1 -->  get data for GOOGL
