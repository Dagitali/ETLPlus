"""Setup configuration for etlplus package."""
from setuptools import find_packages
from setuptools import setup

with open('README.md', encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='etlplus',
    version='0.1.0',
    author='ETLPlus Team',
    description='A Swiss Army knife for enabling simple ETL operations',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/Dagitali/ETLPlus',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    python_requires='>=3.8',
    install_requires=[
        'requests>=2.25.0',
        'pandas>=1.2.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=3.0.0',
            'black>=22.0.0',
            'flake8>=4.0.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'etlplus=etlplus.__main__:main',
        ],
    },
)
