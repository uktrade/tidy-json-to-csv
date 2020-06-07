from collections import defaultdict
import codecs
import csv
import re
import ijson


def to_csvs(json_bytes, save_csv_bytes, null='#NA'):
    top_level_saved = defaultdict(set)
    open_maps = {}
    parent_ids = []
    open_csv_gens = {}

    class PseudoBuffer:
        def write(self, value):
            return value

    csv_writer = csv.writer(PseudoBuffer(), quoting=csv.QUOTE_NONNUMERIC)

    def save(path, dict_data):
        try:
            gen = open_csv_gens[path]
        except KeyError:
            gen = save_csv_bytes(path)
            open_csv_gens[path] = gen
            next(gen)
            gen.send(codecs.BOM_UTF8)
            gen.send(csv_writer.writerow(dict_data.keys()).encode('utf-8'))

        gen.send(csv_writer.writerow(dict_data.values()).encode('utf-8'))

    def to_path(prefix):
        components = re.findall(r'([^.]+)\.item', prefix)
        return '.'.join([f'{component}[*]' for component in components])

    def handle_start_map(prefix, value):
        open_maps[prefix] = {}

    def handle_end_map(prefix, value):
        # parent, _, key = prefix.rpartition('.')
        key = prefix.rpartition('.item')[0].rpartition('.')[2]
        is_top_level = 'id' in open_maps[prefix]

        # IDs of parents so the user can do JOINs
        parent_id_dict = {
            f'{parent_key}__id': parent_id
            for (parent_key, parent_id) in parent_ids
        }

        # ... and only save these for nested top level
        if is_top_level and len(parent_ids) > 1:
            save(to_path(prefix) + '.id', parent_id_dict)

        # ... but if _not_ top level (i.e. no ID), save the IDs and other data
        if not is_top_level and len(parent_ids):
            save(to_path(prefix), {**parent_id_dict, **open_maps[prefix]})

        # ... and if top level, but not yet saved it, save it
        if is_top_level and open_maps[prefix]['id'] not in top_level_saved[key]:
            save(f'{key}[*]', open_maps[prefix])
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

    def handle_integer(prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

    def handle_double(prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

    def handle_number(prefix, value):
        parent, _, key = prefix.rpartition('.')
        open_maps[parent][key] = value

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
        for gen in open_csv_gens:
            try:
                gen.close()
            except:
                pass
