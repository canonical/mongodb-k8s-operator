# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This file is meant to run in the background continuously writing entries to MongoDB."""
import signal
import sys

from pymongo import MongoClient
from pymongo.errors import (
    AutoReconnect,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
)
from pymongo.write_concern import WriteConcern

run = True


def sigterm_handler(_signo, _stack_frame):
    global run
    run = False


def continous_writes(connection_string: str, starting_number: int):
    write_value = starting_number

    while run:
        client = MongoClient(
            connection_string,
            socketTimeoutMS=5000,
        )
        db = client["continuous_writes_database"]
        test_collection = db["test_collection"]
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
        except (NotPrimaryError, AutoReconnect):
            # this means that the primary was not able to be found. An application should try to
            # reconnect and re-write the previous value. Hence, we `continue` here, without
            # incrementing `write_value` as to try to insert this value again.
            continue
        except OperationFailure as e:
            if e.code == 211:  # HMAC error
                # after cluster comes back up, it needs to resync the HMAC code. Hence, we
                # `continue` here, without incrementing `write_value` as to try to insert
                # this value again.
                continue
            else:
                pass
        except PyMongoError:
            # we should not raise this exception but instead increment the write value and move
            # on, indicating that there was a failure writing to the database.
            pass
        finally:
            client.close()

        write_value += 1

    with open("last_written_value", "w") as fd:
        fd.write(str(write_value - 1))


def main():
    connection_string = sys.argv[1]
    starting_number = int(sys.argv[2])
    continous_writes(connection_string, starting_number)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, sigterm_handler)
    main()
