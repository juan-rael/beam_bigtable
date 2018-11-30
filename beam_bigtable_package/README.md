# Create Package to use in Dataflow

## Description

This code demonstrates how to connect create and upload a extra package to Dataflow and run a code in Dataflow without problems.

## Build and Run
1.  Create the structure. 
    ```    
    ├── beam_bigtable           # Package folder
        ├─── __init__.py        # Package Initialization file.
        ├─── __version__.py     # Version file.
        └─── bigtable.py        # Your Package Code.
    ├── setup.py                # Setup Code
    ├── LICENSE
    └── README.md
    ```
3.  Create the Setup.py using the name of the package create a folder with that name and  then create in that folder a file __init__.py, and import your package file in it.
3.  Run the command to create the file to install the new package.
    ```sh
    $ python setup.py sdist --formats=gztar
    ```
4.  Install your code in your system, because, your going to use it to run your python dataflow code.
    ```sh
    $ pip install beam_bigtable-0.1.1.tar.gz
    ```
5.  Set the arguments in the PipelineOptions
    Need to use extra_package and setup_file arguments.
    extra_package set the path of the compress package.
    setup_file set the file to install this package.
    ```python
    [
        '--experiments=beam_fn_api',
        '--project=grass-clump-479',
        '--requirements_file=requirements.txt',
        '--runner=dataflow',
        '--staging_location=gs://juantest/stage',
        '--temp_location=gs://juantest/temp',
        '--setup_file=./beam_bigtable/setup.py',
        '--extra_package=./beam_bigtable/dist/beam_bigtable-0.1.1.tar.gz'
    ]
    ```

## Contributing changes

* See [CONTRIBUTING.md](../../CONTRIBUTING.md)

## Licensing

* See [LICENSE](../../LICENSE)