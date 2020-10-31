import unittest

from tidy_json_to_csv import to_csvs


class TestIntegration(unittest.TestCase):

    def test_basic(self):
        total_received = {}

        def json_bytes(chunk_size):
          remaining = json_bytes_songs
          while remaining:
              yield remaining[:chunk_size]
              remaining = remaining[chunk_size:]

        def save_csv(path, chunks):
            total_received[path] = []
            for chunk in chunks:
                total_received[path].append(chunk)

        for output_chunk_size in range(1, 200):
            to_csvs(json_bytes(50), save_csv, output_chunk_size=output_chunk_size)
            files = {
                path: b''.join(contents)
                for path, contents in total_received.items()
            }
            self.assertEqual(files, json_bytes_songs_parsed)

        for input_chunk_size in range(1, 200):
            to_csvs(json_bytes(input_chunk_size), save_csv, output_chunk_size=50)
            files = {
                path: b''.join(contents)
                for path, contents in total_received.items()
            }
            self.assertEqual(files, json_bytes_songs_parsed)

json_bytes_songs = b'''{
  "songs": [
    {
      "id": "1",
      "title": "Walk through the fire",
      "categories": [
        {"id": 1, "name": "musicals"},
        {"id": 2, "name": "television-shows"}
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
        {"id": 1, "name": "musicals"},
        {"id": 3, "name": "films"}
      ],
      "comments": [
        {"content": "I also could have danced all night"}
      ],
      "artist": {
        "name": "Dolittle"
      }
    }
  ]
}'''

json_bytes_songs_parsed = {
    'songs[*].categories[*].id': b'"songs__id","categories__id"\r\n"1",1\r\n"1",2\r\n"2",1\r\n"2",3\r\n',
    'songs[*].comments[*]': b'"songs__id","content"\r\n"1","I love it"\r\n"1","I\'ve heard better"\r\n"2","I also could have danced all night"\r\n',
    'songs[*]': b'"id","title","artist__name"\r\n"1","Walk through the fire","Slayer"\r\n"2","I could have danced all night","Dolittle"\r\n',
    'categories[*]': b'"id","name"\r\n1,"musicals"\r\n2,"television-shows"\r\n3,"films"\r\n',
}
