PG2Sqlite Flat copier
=====================

This came about because I was getting frustrated with Postgres.

My data-set had nothing particularly special which needed postgres, except for future scale.

And I'm only trying to understand the schema, not do inserts or anything special.

So instead of running a local postgres & then importing databases with foreign keys, I preferred a shallow sqlite3 dump to read & understand.

To run

	$ python pg2sqlite -c "dbname=foo" -o out.db

and you should have an sqlite file which then can be copied, attached over emails etc.
