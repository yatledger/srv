cargo run -- --id 1 --addr 127.0.0.1 --port 21001
cargo run -- --id 2 --addr 127.0.0.1 --port 21002
cargo run -- --id 3 --addr 127.0.0.1 --port 21003
cargo run -- --id 4 --addr 127.0.0.1 --port 21004

curl -i -H "Content-Type: application/json" \
     -d '' \
     http://127.0.0.1:21002/mng/init

curl -i -H "Content-Type: application/json" \
     -d '[[1, "127.0.0.1:21001"], [2, "127.0.0.1:21002"], [3, "127.0.0.1:21003"], [4, "127.0.0.1:21003"]]' \
     http://127.0.0.1:21001/mng/init

curl -i -H "Content-Type: application/json" \
     -d '[2, "127.0.0.1:21002"]' \
     http://127.0.0.1:21001/mng/add-learner
     
curl -i -H "Content-Type: application/json" \
     -d '[3, "127.0.0.1:21003"]' \
     http://127.0.0.1:21001/mng/add-learner

curl -i -H "Content-Type: application/json" \
     -d '[4, "127.0.0.1:21004"]' \
     http://127.0.0.1:21002/mng/add-learner
     
curl -i -H "Content-Type: application/json" \
     -d '[1, 2, 4]' \
     http://127.0.0.1:21002/mng/change-membership


curl -i -H "Content-Type: application/json" \
     -d '[[1, "127.0.0.1:21001"], [2, "127.0.0.1:21002"], [3, "127.0.0.1:21003"]]' \
     http://127.0.0.1:21001/mng/init

curl -i -H "Content-Type: application/json" \
     -d '' \
     http://127.0.0.1:21001/mng/metrics

curl -i -H "Content-Type: application/json" \
     -d '' \
     http://127.0.0.1:21003/raft/snapshot

