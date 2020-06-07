# tidy-json-to-csv

Convert a subset of JSON to a set of tidy CSVs. Supports both streaming processing of input JSON and output of CSV, and so suitable for large files in memory constrained environments.

> Work in progress. This README serves as a rough design spec.


## Usage

```python
from tidy_json_to_csv import to_csvs

# A save generator must be provided since a single JSON file
# maps to multiple CSVs
def save_csv_bytes(path):
    try:
        with open(f'{path}.csv', 'wb') as f:
            while True:
                f.write(yield)
    except GeneratorExit:
        pass

# Overkill for this example, but shows how a generator can be
# used to generate the bytes of a JSON
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
        {"name": "musicals"},
        {"name": "television-shows"}
      ]
    },
    {
      "id": "2",
      "title": "I could have danced all night",
      "categories": [
        {"name": "musicals"},
        {"name": "films"}
      ]
    }
  ]
}
```

maps to two files, `songs[*].csv` and `songs[*].categories[*].csv`

### `songs[*].csv`

```csv
"id","title"
"1","Walk through the fire"
"2","I could have danced all night"
```

### `songs[*].categories[*].csv`

```csv
"songs.id","name"
"1","musicals"
"1","television-songs"
"2","musicals"
"2","films"
```
