from __future__ import print_function
import sys
import multiprocessing
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from dateutil.parser import parse


class ElasticRepliaction(object):
    """CRUD replication to Elasticsearch"""

    def __init__(self, tables, allow_delete=True, username=None, password=None, connection=None):
        self.db_tables = tables
        self.allow_delete = allow_delete
        self.exclude_columns = []
        if connection:
            print('ES connection to %s ...' % connection, file=sys.stderr)
            self.es = Elasticsearch(connection, http_auth=(username, password)) if username and password else Elasticsearch(connection)
        else:
            print('ES connection to http://localhost:9200/ ...', file=sys.stderr)
            self.es = Elasticsearch()

        print(self.es.info())
        self.table_ids = {}

        def init_values(table):
            print('Creating index %s ...' % table['name'], file=sys.stderr)
            self.table_ids[table['name'].strip()] = table['primary_key']
            self.es.indices.create(index=table['name'].strip(), ignore=400)

            if 'exclude_columns' in table:
                self.exclude_columns += table['exclude_columns'].split(',')

        map(init_values, tables)

    def handle_dates(self, document, column, value):
        try:
            document[column] = parse(value)
        except Exception as e:
            document[column] = value
        return document

    def parse_doc_body(self, document, change):
        data = {}
        for idx, column in enumerate(change['columnnames']):
            if column == document['_id']:
                document['_id'] = change['columnvalues'][idx]
                if type(document['_id']) == str or type(document['_id']) == unicode:
                    document['_id'] = document['_id'].strip()
            elif column not in self.exclude_columns:
                if change['kind'] == 'update':
                    document['_source'] = {}
                    document['_source']['doc'] = self.handle_dates(data, change['columnnames'][idx], change['columnvalues'][idx])
                else:
                    document = self.handle_dates(document, change['columnnames'][idx], change['columnvalues'][idx])
        return document

    def parse_insert_or_update(self, document, change):
        if change['kind'] == 'update':
            document['_op_type'] = 'update'
        else:
            document['_op_type'] = 'create'
        document = self.parse_doc_body(document, change)
        return document

    def parse_delete(self, document, change):
        document['_op_type'] = 'delete'
        for idx, column in enumerate(change['oldkeys']['keynames']):
            if column == document['_id']:
                document['_id'] = change['oldkeys']['keyvalues'][idx]
                if type(document['_id']) == str or type(document['_id']) == unicode:
                    document['_id'] = document['_id'].strip()
                break
        return document

    def replicate(self, data, initial=False, initial_table=None):

        def initial_replicate(entry):
            document = {}
            document['_index'] = initial_table
            document['_type'] = 'document'
            document['_op_type'] = 'create'
            document['_id'] = entry[self.table_ids[initial_table]]
            entry = dict(entry)

            for key, value in entry.iteritems():
                if key not in self.exclude_columns and key != document['_id']:
                    document[key] = value
            return document

        def normal_replicate(change):
            kind = change['kind']
            table = change['table']

            document = {}
            document['_index'] = table
            document['_type'] = 'document'
            document['_id'] = self.table_ids[table]

            if kind in ['delete', 'insert', 'update'] and table in self.table_ids.keys():
                if kind == 'delete':
                    document = self.parse_delete(document, change)
                else:
                    document = self.parse_insert_or_update(document, change)

                return document

        if initial and initial_table:
            try:
                helpers.bulk(self.es, map(initial_replicate, data))
            except Exception as e:
                pass
        else:
            data_to_replicate = map(normal_replicate, data['change'])


            for success, info in helpers.parallel_bulk(self.es, data_to_replicate, thread_count=multiprocessing.cpu_count(), chunk_size=40):
                if not success:
                    print('A document failed:', info)

            print(data_to_replicate)