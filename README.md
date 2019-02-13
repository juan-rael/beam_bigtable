# Read and Write Bigtable with Google Dataflow

## Description

This code demonstrates how to connect,read and write data to bigtable using Google Dataflow.

## Install the Docker

1. You need to create the `Dockerfile`.
```sh
docker build . -t python27_beam_docker
```
2. List the images
```sh
docker image ls
```
3. Run your docker image, with your sharing folder setup(`-v`).
```sh
docker run --security-opt seccomp:unconfined -it -v C:\Users\Juan\Project\python:/usr/src/app python2_beam_docker bash
```
4. Go to your Work directory.
```sh
cd /usr/src/app/
```
5. Install your dependencies.
```sh
pip install -r requirements.txt
```
6. Add the Google Credentials.
```sh
export GOOGLE_APPLICATION_CREDENTIALS="/usr/src/app/test_beam/grass-clump-479-clients-dev.json"
```
7. Create the Bigtable extra file Package to upload to Dataflow.
    [Create Extra Package](https://github.com/juan-rael/beam_bigtable/tree/master/beam_bigtable)
8. Run the example.py setting the project, instance and table arguments.
```sh
python test_million_rows_read.py
```
9. Go to Dataflow API.

## Contributing changes

* See [CONTRIBUTING.md](../../CONTRIBUTING.md)

## Licensing

* See [LICENSE](../../LICENSE)