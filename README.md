# tidy-json-to-csv [![CircleCI](https://circleci.com/gh/uktrade/tidy-json-to-csv.svg?style=svg)](https://circleci.com/gh/uktrade/tidy-json-to-csv)

Converts a subset of JSON to a set of tidy CSVs. Supports both streaming processing of input JSON and output of CSV, and so suitable for large files in memory constrained environments.


## What problem does this solve?

Most JSON to CSV converters do not result in data suitable for immediate analysis. They usually output a single CSV, and to do this, result in some combination of:

- JSON inside CSV fields;
- values in lists presented as columms;
- data duplicated in multiple rows / a row's position in the CSV determines its context.

Often these require subsequent manual, and so error-prone, data manipulation. This library aims to do all the conversion up-front, so you end up with a set of [tidy](https://vita.had.co.nz/papers/tidy-data.pdf) tables, which is often a great place from which to start analysis.


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
      ],
      "artist": {
        "name": "Slayer"
      }
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
      ],
      "artist": {
        "name": "Doolitle"
      }
    }
  ]
}
```

maps to four files:

### `songs.csv`

```csv
"id","title","artist__name"
"1","Walk through the fire","Slayer"
"2","I could have danced all night","Doolitle"
```

### `songs__categories__id.csv`

```csv
"songs__id","categories__id"
"1","1"
"1","2"
"2","1"
"2","3"
```

### `songs__comments.csv`

```csv
"songs__id","content"
"1","I love it"
"1","I've heard better"
"2","I also could have danced all night"
```

### `categories.csv`

```csv
"id","name"
"1","musicals"
"2","television-shows"
"3","films"
```


## Installation

```bash
pip install tidy-json-to-csv
```


## Usage: Convert JSON to multiple CSV files (Command line)

```bash
cat songs.json | tidy_json_to_csv
```


## Usage: Convert JSON to multiple CSV files (Python)

```python
from tidy_json_to_csv import to_csvs

# A save function, called by to_csvs for each CSV file to be generated.
# Will be run in a separate thread, started by to_csvs
def save_csv_bytes(path, chunks):
    with open(f'{path}.csv', 'wb') as f:
        for chunk in chunks:
            f.write(chunk)

def json_bytes():
    with open(f'file.json', 'rb') as f:
        chunk = f.read(65536)
        if chunk:
            yield chunk

to_csvs(json_bytes(), save_csv_bytes, null='#NA', output_chunk_size=65536)
```


## Usage: Convert JSON to multiple Pandas data frames (Python)

```python
import io
import queue

import pandas as pd
from tidy_json_to_csv import to_csvs

def json_to_pandas(json_filename):
    q = queue.Queue()

    class StreamedIterable(io.RawIOBase):
        def __init__(self, iterable):
            self.iterable = iterable
            self.remainder = b''
        def readable(self):
            return True
        def readinto(self, b):
            buffer_size = len(b)

            while len(self.remainder) < buffer_size:
                try:
                    self.remainder = self.remainder + next(self.iterable)
                except StopIteration:
                    if self.remainder:
                        break
                    return 0

            chunk, self.remainder = self.remainder[:buffer_size], self.remainder[buffer_size:]
            b[:len(chunk)] = chunk
            return len(chunk)

    def save_csv_bytes(path, chunks):
        q.put((path, pd.read_csv(io.BufferedReader(StreamedIterable(chunks), buffer_size=65536), na_values=['#NA'])))

    def json_bytes():
        with open(json_filename, 'rb') as f:
            chunk = f.read(65536)
            if chunk:
                yield chunk

    to_csvs(json_bytes(), save_csv_bytes, null='#NA')

    dfs = {}
    while not q.empty():
        path, df = q.get()
        dfs[path] = df

    return dfs

dfs = json_to_pandas('songs.json')
for path, df in dfs.items():
    print(path)
    print(df)
```


## Constraints

Denormalised input JSON is assumed, and the output is normalised. If a nested object has an `id` field, it is assumed to be the primary key of a top-level table. All objects that have a nested object or array _must_ have an `id` field that serves as its primary key in the final output. If present, `id` must be the _first_ key in a map. All arrays must be arrays of objects rather than primitives.

Although _mostly_ streaming, to support denormalised input JSON and to avoid repeating the same rows in normalised CSVs, an internal record of output IDs is maintained during processing.
