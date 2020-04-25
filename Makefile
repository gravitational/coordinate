.PHONY: test stop

TEST_ETCD_IMAGE := quay.io/coreos/etcd:v3.3.12
TEST_ETCD_INSTANCE := coordinate0

test:
	if docker ps | grep $(TEST_ETCD_INSTANCE) --quiet; then \
	  echo "ETCD is already running"; \
	else \
	  echo "starting test ETCD instance"; \
	  etcd_instance=$(shell docker ps -a | grep $(TEST_ETCD_INSTANCE) | awk '{print $$1}'); \
	  if [ "$$etcd_instance" != "" ]; then \
	    docker rm -v $$etcd_instance; \
	  fi; \
	  docker run --name=$(TEST_ETCD_INSTANCE) \
	  	--publish 34001:4001 \
		--publish 32380:2380 \
		--publish 32379:2379 \
		--env "ETCD_API=2" \
		--detach $(TEST_ETCD_IMAGE) \
		etcd -name etcd0 \
			-debug \
			-logger=zap \
			--enable-v2=true \
			-listen-client-urls=http://0.0.0.0:2379,http://0.0.0.0:4001 \
			-advertise-client-urls http://localhost:32379,http://localhost:34001; \
	fi;
	COORDINATE_TEST_ETCD_NODES=http://localhost:34001 go test -mod=vendor -count=1 -race ./... -check.f=$(TC)

stop:
	docker stop $(TEST_ETCD_INSTANCE) && docker rm -v $(TEST_ETCD_INSTANCE)
