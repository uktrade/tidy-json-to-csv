from collections import defaultdict
import codecs
import concurrent.futures
import csv
import re
import ijson
import queue
import sys


def to_csvs(json_bytes, save_csv_bytes, null='#NA', output_chunk_size=65536, save_chunk_timeout=5, max_files=1024):
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

    def save(executor, path, dict_data):
        try:
            f, q = open_csv_qs[path]
        except KeyError:
            if len(open_csv_qs) >= max_files:
                raise Exception('Too many open files')

            q = queue.Queue(maxsize=1)
            f = executor.submit(save_csv_bytes, path, buffer(QueuedIterable(q)))
            open_csv_qs[path] = (f, q)
            q.put(csv_writer.writerow(dict_data.keys()).encode('utf-8'), timeout=save_chunk_timeout)

        q.put(csv_writer.writerow(dict_data.values()).encode('utf-8'), timeout=save_chunk_timeout)

    def to_path(prefix):
        return re.sub(r'([^.]+)\.item\.?', r'\1__', prefix).rstrip('_')

    def handle_start_map(executor, prefix, value):
        open_maps[prefix] = {}

    def handle_end_map(executor, prefix, value):
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
            save(executor, to_path(prefix) + '__id', parent_id_dict)

        # ... but if _not_ top level (i.e. no ID), save the IDs and other data
        if not is_sub_object and not is_top_level and len(parent_ids):
            save(executor, to_path(prefix), {**parent_id_dict, **open_maps[prefix]})

        # ... and if top level, but not yet saved it, save it
        if not is_sub_object and is_top_level and open_maps[prefix]['id'] not in top_level_saved[key]:
            save(executor, f'{key}', open_maps[prefix])
            top_level_saved[key].add(open_maps[prefix]['id'])

        # We're going to be moving up a level, so no need for last ID
        if is_top_level:
            parent_ids.pop()

        del open_maps[prefix]

    def handle_map_key(executor, prefix, value):
        pass

    def handle_start_array(executor, prefix, value):
        pass

    def handle_end_array(executor, prefix, value):
        pass

    def handle_null(executor, prefix, _):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = null

    def handle_boolean(executor, prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

    def handle_number(executor, prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

        if key == 'id':
            parent_key = prefix.rpartition('.item.')[0].rpartition('.item.')[2]
            parent_ids.append((parent_key, value))

    def handle_string(executor, prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

        if key == 'id':
            parent_key = prefix.rpartition('.item.')[0].rpartition('.item.')[2]
            parent_ids.append((parent_key, value))

    handlers = locals()

    def process(executor, events):
        for prefix, event, value in events:
            handlers[f'handle_{event}'](executor, prefix, value)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_files) as executor:
        try:
            events = ijson.sendable_list()
            coro = ijson.parse_coro(events)
            for chunk in json_bytes:
                coro.send(chunk)
                process(executor, events)
                del events[:]

            coro.close()
            process(executor, events)

        # Close all open CSVs
        finally:
            for _, q in open_csv_qs.values():
                try:
                    q.put(STOP_SENTINAL, timeout=save_chunk_timeout)
                except:
                    pass

            for f, _ in open_csv_qs.values():
                exception = f.exception(timeout=save_chunk_timeout)
                if exception:
                    raise exception

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
