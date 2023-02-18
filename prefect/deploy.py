from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from nyc_taxi_data_pipeline.prefect.flows.load_trips_data import load_trips_data

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="asrst/nyc-taxi-prefect-etl:v1",  # insert your image here
    image_pull_policy="ALWAYS",
    network_mode="bridge",
    auto_remove=True,
)

docker_block.save("nyc-taxi-etl", overwrite=True)

# docker_block = DockerContainer.load("nyc-taxi-etl")

docker_dep = Deployment.build_from_flow(
    flow=load_trips_data,
    name="docker-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()