# tidy-json-to-csv

Convert a subset of JSON to a set of tidy CSVs. Supports both streaming processing of input JSON and output of CSV, and so suitable for large files in memory constrained environments.

> Work in progress. This README serves as a rough design spec.


## Usage

```python
from tidy_json_to_csv import to_csv

def save(path, bytes_gen):
  # A save function must be provided since a single JSON file maps to multiple CSVs
  with open(f'{path}.csv', 'wb') as f:
    for bytes_chunk in bytes_gen:
      f.write(chunk)

def json_gen():
  # Overkill for this example, but shows how a generator can be used to stream
  with open(f'file.json', 'rb') as f:
    yield f.read()

to_csv(json_gen(), save, null='#NA')
```
