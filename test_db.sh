docker stop cockroach-sorting-hat && docker rm cockroach-sorting-hat

docker run --name cockroach-sorting-hat -d -p 5434:26257 cockroachdb/cockroach:v21.1.7 start-single-node --insecure

echo "create database sorting_hat;" | docker run -i --net=host --rm cockroachdb/cockroach:v21.1.7 sql --insecure --host localhost --port 5434

cat init.sql | docker run -i --net=host --rm cockroachdb/cockroach:v21.1.7 sql --insecure --host localhost --port 5434 --database sorting_hat
