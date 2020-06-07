# tidy-json-to-csv [![CircleCI](https://circleci.com/gh/uktrade/tidy-json-to-csv.svg?style=svg)](https://circleci.com/gh/uktrade/tidy-json-to-csv)

Converts a subset of JSON to a set of tidy CSVs. Supports both streaming processing of input JSON and output of CSV, and so suitable for large files in memory constrained environments.

Denormalised input JSON is assumed, and the output is normalised. If a nested object has an `id` field, it is assumed to be the primary key of a top-level table. All objects that have a nested object or array _must_ have an `id` field that serves as its primary key in the final output. If present, `id` must be the _first_ key in a map. All arrays must be arrays of objects rather than primitives.

Although _mostly_ streaming, to support denormalised input JSON and to avoid repeating the same rows in normalised CSVs, an internal record of output IDs is maintained during processing.


## Installation

```bash
pip install tidy-json-to-csv
```


## Usage

```python
from tidy_json_to_csv import to_csvs

# A save generator must be provided since a single JSON file
# maps to multiple CSVs
def save_csv_bytes(path):
    with open(f'{path}.csv', 'wb') as f:
        while True:
            chunk = yield
            f.write(chunk)

# Overkill for this example, but shows how a generator can be
# used to generate the bytes of a large JSON file
def json_bytes():
    with open(f'file.json', 'rb') as f:
        yield f.read()

to_csvs(json_bytes(), save_csv_bytes, null='#NA')
```


## Example input and output

The JSON

```json
{
  "songs": [
    {
      "id": "1",
      "title": "Walk through the fire",
      "categories": [
        {"id": "1", "name": "musicals"},
        {"id": "2", "name": "television-shows"}
      ],
      "comments": [
        {"content": "I love it"},
        {"content": "I've heard better"}
      ]
    },
    {
      "id": "2",
      "title": "I could have danced all night",
      "categories": [
        {"id": "1", "name": "musicals"},
        {"id": "3", "name": "films"}
      ],
      "comments": [
        {"content": "I also could have danced all night"}
      ]
    }
  ]
}
```

maps to four files:

### `songs[*].csv`

```csv
"id","title"
"1","Walk through the fire"
"2","I could have danced all night"
```

### `songs[*].categories[*].id.csv`

```csv
"songs.id","categories.id"
"1","1"
"1","2"
"2","1"
"2","3"
```

### `songs[*].comments[*].csv`

```csv
"songs.id","name"
"1","I love it"
"1","I've heard better"
"2","I also could have danced all night"
```

### `categories[*].csv`

```csv
"id","name"
"1","musicals"
"2","television-shows"
"3","films"
```
