IMAGE_PREFIX = twosixlabsdart
IMAGE_NAME = java-kafka-consumer
IMG := $(IMAGE_PREFIX)/$(IMAGE_NAME)
TAG = latest

docker-build:
	mvn clean package
	docker build -t $(IMG):$(TAG) .

docker-push: docker-build
	docker push $(IMG):$(TAG)
