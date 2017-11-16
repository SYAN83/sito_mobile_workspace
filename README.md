# sito_mobile_workspace

## Prerequisites

- Git

- Docker
  - Docker for Mac: https://docs.docker.com/docker-for-mac/install/
  - Docker for Windows: https://docs.docker.com/docker-for-windows/install/
  - Docker for Ubuntu: https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/

## Installation

- Clone the GitHub repo

  ```
  git clone https://github.com/SYAN83/sito_mobile_workspace.git
  ```

- Change your working directory into the `sito_mobile_workspace` directory, and build then the docker image (it might take a while for the first time)

  ```
  cd sito_mobile_workspace/
  docker build -t sito_workspace .
  ```
 
- Create a file `config.yml` using `config_template.yml` as the template, fill in AWS credentials and bucket name, and save it under `Workspace/` subdirectory.

## Starting Docker Container
- In the same directory, run docker image by executing the following command:

  ```
  docker run -it --mount type=bind,source="$PWD"/Workspace,target=/home/jovyan/work  -p 8888:8888 sito_workspace:latest
  ```
 
- When the notebook is running, copy/paste the URL into your browser to run.

## Stoping Docker Container

- To stop Container, first get the CONTAINER ID by executing

  ```
  docker container ls
  ```

- Run the following command to stop the container

  ```
  docker container stop <CONTAINER ID>
  ```
  
- (Optional) Removing container

  ```
  docker container rm <CONTAINER ID> --force
  ```
