# BGZip in Postgres

We provide a C module, as a Postgres extension, to block-gzip compress and gzip compress data

	make
	make install

And create the extension

	psql 'postgresql://superuser@localhost:5432/database' -c "CREATE EXTENSION pg_bgzip;"

It depends on `libdeflate`
