#!/bin/sh

echo=$(which echo)
$echo -n "UPDATE pg_index SET indisprimary = 't' \
	WHERE indexrelid IN (" ;
$AWK  '
	/PRIMARY KEY/ {
		if (match($0, /PRIMARY KEY [0-9]+/))
			if (match($0, /[0-9]+/))
				printf "%s, ", substr($0, RSTART, RLENGTH)
	}
	'

$echo "0)"
