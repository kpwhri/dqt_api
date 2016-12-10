from distutils.core import setup
import setuptools
import os

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'README.md'),
          encoding='utf-8') as f:
    long_description = f.read()

setup(name='data_quality_tool',
      version='0.0.1',
      description='Simple backend API for a data query tool.',
      long_description=long_description,
      url='https://bitbucket.org/dcronkite/dqt_api',
      author='dcronkite',
      author_email='dcronkite@gmail.com',
      license='MIT',
      classifiers=[  # from https://pypi.python.org/pypi?%3Aaction=list_classifiers
          'Development Status :: 1 - Planning',
          'Intended Audience :: Science/Research',
          'Programming Language :: Python :: 3 :: Only',
      ],
      keywords='fitbit',
      entry_points={
          'console_scripts':
              [
              ]
      },
      package_dir={'': 'src'},
      include_package_data=True,
      packages=setuptools.find_packages('src'),
      zip_safe=False
      )