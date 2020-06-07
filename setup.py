import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='tidy-json-to-csv',
    version='0.0.1',
    author='Department for International Trade',
    author_email='webops@digital.trade.gov.uk',
    description='Convert JSON to a set of tidy CSV files',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/tidy-json-to-csv',
    py_modules=[
        'tidy_json_to_csv',
    ],
    python_requires='>=3.6.3',
    install_requires=[
        'ijson>=3.0.4,<4',
    ],
    test_suite='tests',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ]
)
