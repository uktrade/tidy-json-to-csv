import unittest

from tidy_json_to_csv import to_csvs


class TestIntegration(unittest.TestCase):

    def test_basic(self):
        total_received = {}

        def json_bytes():
            yield json_bytes_songs

        def save_csv(path):
            total_received[path] = []
            while True:
                bytes_chunked = yield
                total_received[path].append(bytes_chunked)

        to_csvs(json_bytes(), save_csv)
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
}'''

json_bytes_songs_parsed = {
    'songs[*].categories[*].id': b'\xef\xbb\xbf"songs__id","categories__id"\r\n"1","1"\r\n"1","2"\r\n"2","1"\r\n"2","3"\r\n',
    'songs[*].comments[*]': b'\xef\xbb\xbf"songs__id","content"\r\n"1","I love it"\r\n"1","I\'ve heard better"\r\n"2","I also could have danced all night"\r\n',
    'songs[*]': b'\xef\xbb\xbf"id","title"\r\n"1","Walk through the fire"\r\n"2","I could have danced all night"\r\n',
    'categories[*]': b'\xef\xbb\xbf"id","name"\r\n"1","musicals"\r\n"2","television-shows"\r\n"3","films"\r\n',
}
