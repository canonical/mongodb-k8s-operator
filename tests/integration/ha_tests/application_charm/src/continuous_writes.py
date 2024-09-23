# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This file is meant to run in the background continuously writing entries to MongoDB."""
import sys

import signal
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern

DEFAULT_DB_NAME = "new-db"
DEFAULT_COLL_NAME = "test_collection"

run = True


def sigterm_handler(_signo, _stack_frame):
    global run
    run = False


def continous_writes(
    connection_string: str,
    starting_number: int,
    db_name: str,
    coll_name: str,
):
    write_value = starting_number

    while run:
        client = MongoClient(
            connection_string,
            socketTimeoutMS=5000,
        )
        db = client[db_name]
        test_collection = db[coll_name]
        try:
            # insert item into collection if it doesn't already exist
            test_collection.with_options(
                write_concern=WriteConcern(
                    w="majority",
                    j=True,
                    wtimeout=1000,
                )
            ).update_one({"number": write_value}, {"$set": {"number": write_value}}, upsert=True)

            # update_one
        except PyMongoError:
            # PyMongoErors should result in an attempt to retry a write. An application should
            # try to reconnect and re-write the previous value. Hence, we `continue` here, without
            # incrementing `write_value` as to try to insert this value again.
            continue
        finally:
            client.close()

        write_value += 1

    with open(f"last_written_value-{db_name}-{coll_name}", "w") as fd:
        fd.write(str(write_value - 1))


def main():
    connection_string = sys.argv[1]
    starting_number = int(sys.argv[2])
    db_name = DEFAULT_DB_NAME if len(sys.argv) < 4 else sys.argv[3]
    coll_name = DEFAULT_COLL_NAME if len(sys.argv) < 5 else sys.argv[4]
    continous_writes(connection_string, starting_number, db_name, coll_name)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, sigterm_handler)
    main()
