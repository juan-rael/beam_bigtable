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
> the first element in the -v argument is the windows shared path and the second is your path in the docker container.(you need to separate it with a :).
> python2_beam_docker is the name of your docker image.
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
export GOOGLE_APPLICATION_CREDENTIALS="/path/of/your/credential/file/file.json"
```
## Create the Bigtable extra file Package to upload to Dataflow.
1. This code demonstrates how to connect create and upload a extra package to Dataflow and run a code in Dataflow without problems.

    ```
    ├── beam_bigtable           # Package folder
        ├─── __init__.py        # Package Initialization file.
        ├─── __version__.py     # Version file.
        └─── bigtable.py        # Your Package Code.
    ├── setup.py                # Setup Code
    ├── LICENSE
    └── README.md
    ```
2. Create the Setup.py using the name of the package create a folder with that name and  then create in that folder a file __init__.py, and import your package file in it.
3.Run the command to create the file to install the new package.
    ```sh
    $ python setup.py sdist --formats=gztar
    ```
4. Install your code in your system, because, your going to use it to run your python dataflow code.
    ```sh
    $ pip install beam_bigtable-0.1.1.tar.gz
    ```
5. Set the arguments in the PipelineOptions
    Need to use extra_package and setup_file arguments.
    extra_package set the path of the compress package.
    setup_file set the file to install this package.
    ```python
    [
        '--experiments=beam_fn_api',
        '--project=project-id',
        '--requirements_file=requirements.txt',
        '--runner=dataflow',
        '--staging_location=gs://storage-instance/stage',
        '--temp_location=gs://storage-instance/temp',
        '--setup_file=./beam_bigtable/setup.py',
        '--extra_package=./beam_bigtable/dist/beam_bigtable-0.1.1.tar.gz'
    ]
    ```

## Run the examples
Run the Read Example
```sh
python test_million_rows_read.py
```
Run the Write Example
```sh
python test_million_rows_write.py
```
9. Go to Dataflow API.

## Contributing changes

* See [CONTRIBUTING.md](../../CONTRIBUTING.md)

## Licensing

* See [LICENSE](../../LICENSE)