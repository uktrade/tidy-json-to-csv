from collections import defaultdict
import codecs
import csv
import re
import ijson
import queue
import sys
import threading


def to_csvs(json_bytes, save_csv_bytes, null='#NA', output_chunk_size=65536):
    STOP_SENTINAL = object()
    top_level_saved = defaultdict(set)
    open_maps = {}
    parent_ids = []
    open_csv_qs = {}

    class PseudoBuffer:
        def write(self, value):
            return value

    csv_writer = csv.writer(PseudoBuffer(), quoting=csv.QUOTE_NONNUMERIC)

    class QueuedIterable():
        def __init__(self, q):
            self.q = q

        def __iter__(self):
            return self

        def __next__(self):
            item = self.q.get()
            self.q.task_done()
            if item is STOP_SENTINAL:
                raise StopIteration()
            return item

    def buffer(chunks):
        queue = []
        queue_length = 0

        for chunk in chunks:
            queue.append(chunk)
            queue_length += len(chunk)

            while queue_length >= output_chunk_size:
                to_send_later = b''.join(queue)
                chunk, to_send_later = \
                    to_send_later[:output_chunk_size], to_send_later[output_chunk_size:]

                queue = \
                    [to_send_later] if to_send_later else \
                    []
                queue_length = len(to_send_later)

                yield chunk

        if queue_length:
            yield b''.join(queue)

    def save(path, dict_data):
        try:
            _, q = open_csv_qs[path]
        except KeyError:
            q = queue.Queue(maxsize=1)
            t = threading.Thread(target=save_csv_bytes, args=(path, buffer(QueuedIterable(q))))
            t.start()
            open_csv_qs[path] = (t, q)
            q.put(csv_writer.writerow(dict_data.keys()).encode('utf-8'))

        q.put(csv_writer.writerow(dict_data.values()).encode('utf-8'))

    def to_path(prefix):
        return re.sub(r'([^.]+)\.item\.?', r'\1__', prefix).rstrip('_')

    def handle_start_map(prefix, value):
        open_maps[prefix] = {}

    def handle_end_map(prefix, value):
        key = prefix.rpartition('.item')[0].rpartition('.')[2]
        is_top_level = 'id' in open_maps[prefix]
        is_sub_object = not prefix.endswith('.item')

        # If a plain object, append to parent
        if is_sub_object:
            parent_prefix = prefix[:prefix.rfind('.')]
            sub_object_key = prefix[prefix.rfind('.') + 1:]
            parent = open_maps[parent_prefix]
            for sub_value_key, value in open_maps[prefix].items():
                parent[sub_object_key + '__' + sub_value_key] = value

        # IDs of parents so the user can do JOINs
        parent_id_dict = {
            f'{parent_key}__id': parent_id
            for (parent_key, parent_id) in parent_ids
        }

        # ... and only save these for nested top level
        if not is_sub_object and is_top_level and len(parent_ids) > 1:
            save(to_path(prefix) + '__id', parent_id_dict)

        # ... but if _not_ top level (i.e. no ID), save the IDs and other data
        if not is_sub_object and not is_top_level and len(parent_ids):
            save(to_path(prefix), {**parent_id_dict, **open_maps[prefix]})

        # ... and if top level, but not yet saved it, save it
        if not is_sub_object and is_top_level and open_maps[prefix]['id'] not in top_level_saved[key]:
            save(f'{key}', open_maps[prefix])
            top_level_saved[key].add(open_maps[prefix]['id'])

        # We're going to be moving up a level, so no need for last ID
        if is_top_level:
            parent_ids.pop()

        del open_maps[prefix]

    def handle_map_key(prefix, value):
        pass

    def handle_start_array(prefix, value):
        pass

    def handle_end_array(prefix, value):
        pass

    def handle_null(prefix, _):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = null

    def handle_boolean(prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

    def handle_number(prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

        if key == 'id':
            parent_key = prefix.rpartition('.item.')[0].rpartition('.item.')[2]
            parent_ids.append((parent_key, value))

    def handle_string(prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

        if key == 'id':
            parent_key = prefix.rpartition('.item.')[0].rpartition('.item.')[2]
            parent_ids.append((parent_key, value))

    handlers = locals()

    def process(events):
        for prefix, event, value in events:
            handlers[f'handle_{event}'](prefix, value)

    try:
        events = ijson.sendable_list()
        coro = ijson.parse_coro(events)
        for chunk in json_bytes:
            coro.send(chunk)
            process(events)
            del events[:]

        coro.close()
        process(events)

    # Close all open CSVs
    finally:
        for _, q in open_csv_qs.values():
            q.put(STOP_SENTINAL)

        for t, _ in open_csv_qs.values():
            t.join()


def main():
    def json_bytes_from_stdin():
        while True:
            chunk = sys.stdin.buffer.read(65536)
            if not chunk:
                break
            yield chunk

    def save_csv_bytes(path, chunks):
        with open(f'{path}.csv', 'wb') as f:
            for chunk in chunks:
                f.write(chunk)

    to_csvs(json_bytes_from_stdin(), save_csv_bytes)


if __name__ == '__main__':
    main()
