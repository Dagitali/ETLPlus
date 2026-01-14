"""
ETLPlus Packaging
=================

Setuptools configuration for the ``etlplus`` package.

Notes
-----
- Reads the project README as the long description.
- Declares console entry point ``etlplus`` -> ``etlplus.__main__:main``.
- Requires Python >= 3.8 (see ``python_requires``).
- Development extras are provided under ``extras_require['dev']``.
"""

from setuptools import find_packages  # type: ignore[import]
from setuptools import setup

# SECTION: SETUP ============================================================ #


with open('README.md', encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='etlplus',
    use_scm_version=True,
    setup_requires=['setuptools-scm'],
    author='ETLPlus Team',
    description='A Swiss Army knife for enabling simple ETL operations',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/Dagitali/ETLPlus',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'etlplus': ['templates/*.j2'],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
    ],
    python_requires='>=3.13,<3.15',
    install_requires=[
        'fastavro>=1.12.1',
        'jinja2>=3.1.6',
        'openpyxl>=3.1.5',
        'pandas>=2.3.3',
        'pydantic>=2.12.5',
        'pyodbc>=5.3.0',
        'pyarrow>=22.0.0',
        'PyYAML>=6.0.3',
        'python-dotenv>=1.2.1',
        'requests>=2.32.5',
        'SQLAlchemy>=2.0.45',
        'typer>=0.21.0',
        'xlrd>=2.0.2',
        'xlwt>=1.3.0',
    ],
    extras_require={
        'dev': [
            'PyYAML>=6.0.3',
            'pytest>=8.4.2',
            'pytest-cov>=7.0.0',
            'black>=25.9.0',
            'flake8>=7.3.0',
            'ruff>=0.14.4',
            'pydocstyle>=6.3.0',
            'pydoclint>=0.8.1',
        ],
    },
    entry_points={
        'console_scripts': [
            'etlplus=etlplus.cli:main',
        ],
    },
)
