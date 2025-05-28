docker_image := iomete.azurecr.io/iomete/iomete_data_compaction
docker_tag := 1.2.6

test:
	pytest --capture=no --log-cli-level=DEBUG

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
