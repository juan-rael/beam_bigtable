import os
import setuptools


setuptools.setup(
	name='beam_bigtable',
	version='0.0.2',
	install_requires=[
		'google-cloud-bigtable==0.31.0',
		'google-cloud-core==0.28.1'
	],
	scripts=[
		'__init__.py',
		'bigtable.py'
	],
	include_package_data=True,
	packages=setuptools.find_packages(),
)