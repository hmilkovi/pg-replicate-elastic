## Replication PostreSQL to Elasticsearch via Logical replication slots

### Demo
[![asciicast](https://asciinema.org/a/177922.svg)](https://asciinema.org/a/177922)


### Install (Python2)

```
$ pip install pg-replicate-elastic
```

#### wal2json

*Note:* Use my fork as we tested it to be 100% sure it work with it and no breaking changes happen, [wal2json](https://github.com/hmilkovi/wal2json) .

```
$ git clone https://github.com/hmilkovi/wal2json.git
$ PATH=/path/to/bin/pg_config:$PATH
$ USE_PGXS=1 make
$ USE_PGXS=1 make install
```

You need to set up at least two parameters at postgresql.conf:
```
wal_level = logical
max_replication_slots = 1
```
After changing these parameters, a restart is needed.

### Usage

```
pg_replicate_elastic --config=<absolute path to json config>
```

Construct configuration file in json format where:

* **replication_slot** json object for replication slot name and if is temporary
* **tables** is array of tables we want to replicate
* **es_connection** connection string to Elasticsearch
* **postgres** json object for PostreSQL connection
* **inital_sync** boolean for inital syncronization that needs to be done first time
to replicate old data

Example configuration
```
{
	"replication_slot": {
		"name": "elasticsearch_slot",
		"is_temp": true
	},
	"tables": [{
		"name": "poc",
		"primary_key": "a"
		"exclude_columns": "c,z"
	}],
	"es_connection": "http://127.0.0.1:9200/",
	"postgres": {
		"port": 5432,
		"host": "127.0.0.1",
		"database": "poc",
		"username": "test",
		"password": "test"
	},
	"inital_sync": false
}
```

### Note

wal2json is not my software so for licence check [their licence](https://raw.githubusercontent.com/hmilkovi/wal2json/master/LICENSE)
