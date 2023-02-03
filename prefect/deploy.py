from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from flows.web_to_gcs import etl_web_to_gcs

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="asrst/prefect:etl_web_to_gcs",  # insert your image here
    image_pull_policy="ALWAYS",
    network_mode="bridge",
    auto_remove=True,
)

docker_block.save("nyc-taxi-etl", overwrite=True)

# docker_block = DockerContainer.load("nyc-taxi-etl")

docker_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="docker-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()