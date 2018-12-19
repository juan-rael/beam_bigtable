from __future__ import absolute_import
import argparse
import logging
import re

import apache_beam as beam
from past.builtins import unicode
from apache_beam.io import iobase
from google.cloud import bigtable
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
from apache_beam.io.iobase import SourceBundle

from apache_beam.transforms.display import HasDisplayData
from google.cloud.bigtable.batcher import MutationsBatcher
from apache_beam.transforms.display import DisplayDataItem
from beam_bigtable.bigtable import BigtableReadConfiguration

from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

from beam_bigtable.bigtable import ReadFromBigtable

from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange


project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

class ReadBigtableOptions(SetupOptions):
	@classmethod
	def _add_argparse_args(cls, parser):
		parser.add_argument(
			'--requirements_file',
			default=None,
			help=
			('Path to a requirements file containing package dependencies. '
			 'Typically it is produced by a pip freeze command. More details: '
			 'https://pip.pypa.io/en/latest/reference/pip_freeze.html. '
			 'If used, all the packages specified will be downloaded, '
			 'cached (use --requirements_cache to change default location), '
			 'and then staged so that they can be automatically installed in '
			 'workers during startup. The cache is refreshed as needed '
			 'avoiding extra downloads for existing packages. Typically the '
			 'file is named requirements.txt.'))
		parser.add_argument(
			'--requirements_cache',
			default=None,
			help=
			('Path to a folder to cache the packages specified in '
			 'the requirements file using the --requirements_file option.'))
		parser.add_argument(
			'--setup_file',
			default=None,
			help=
			('Path to a setup Python file containing package dependencies. If '
			 'specified, the file\'s containing folder is assumed to have the '
			 'structure required for a setuptools setup package. The file must be '
			 'named setup.py. More details: '
			 'https://pythonhosted.org/an_example_pypi_project/setuptools.html '
			 'During job submission a source distribution will be built and the '
			 'worker will install the resulting package before running any custom '
			 'code.'))
		parser.add_argument(
			'--beam_plugin', '--beam_plugin',
			dest='beam_plugins',
			action='append',
			default=None,
			help=
			('Bootstrap the python process before executing any code by importing '
			 'all the plugins used in the pipeline. Please pass a comma separated'
			 'list of import paths to be included. This is currently an '
			 'experimental flag and provides no stability. Multiple '
			 '--beam_plugin options can be specified if more than one plugin '
			 'is needed.'))
		parser.add_argument(
			'--save_main_session',
			default=False,
			action='store_true',
			help=
			('Save the main session state so that pickled functions and classes '
			 'defined in __main__ (e.g. interactive session) can be unpickled. '
			 'Some workflows do not need the session state if for instance all '
			 'their functions/classes are defined in proper modules (not __main__)'
			 ' and the modules are importable in the worker. '))
		parser.add_argument(
			'--sdk_location',
			default='default',
			help=
			('Override the default location from where the Beam SDK is downloaded. '
			 'It can be a URL, a GCS path, or a local path to an SDK tarball. '
			 'Workflow submissions will download or copy an SDK tarball from here. '
			 'If set to the string "default", a standard SDK location is used. If '
			 'empty, no SDK is copied.'))
		parser.add_argument(
			'--extra_package', '--extra_packages',
			dest='extra_packages',
			action='append',
			default=None,
			help=
			('Local path to a Python package file. The file is expected to be (1) '
			 'a package tarball (".tar"), (2) a compressed package tarball '
			 '(".tar.gz"), (3) a Wheel file (".whl") or (4) a compressed package '
			 'zip file (".zip") which can be installed using the "pip install" '
			 'command  of the standard pip package. Multiple --extra_package '
			 'options can be specified if more than one package is needed. During '
			 'job submission, the files will be staged in the staging area '
			 '(--staging_location option) and the workers will install them in '
			 'same order they were specified on the command line.'))


argv = [
	'--project=grass-clump-479',
	'--instance=endurance',
	'--table=perf1DFN4UF2'
]

parser = argparse.ArgumentParser()
known_args, pipeline_args = parser.parse_known_args(argv)
pipeline_options = PipelineOptions(pipeline_args)
pipeline_options.view_as(ReadBigtableOptions)