rm -rf ~/.tendermint
make build
./build/tendermint init
./build/tendermint start --proxy_app=kvstore

