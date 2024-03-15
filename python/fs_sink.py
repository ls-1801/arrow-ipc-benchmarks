import argparse
import os
import time
from common import *

import numpy as np
import pyarrow
import pyarrow.ipc as ipc
import pyarrow.feather as feather  # For Feather file compatibility (optional)


def read_arrow_ipc_stream(reader, counter):
    """Reads Arrow data from a stream and prints int64 values at each row."""


def main():
    """Continuously monitors for new Arrow IPC files and reads them."""
    parser = argparse.ArgumentParser(
        prog='fs_source.py'
    )

    parser.add_argument('schemaName')
    parser.add_argument('filename')
    parser.add_argument('bufferPerFile')
    parser.add_argument('bufferSize')

    args = parser.parse_args()

    schema_name = args.schemaName.lower()
    filename = args.filename
    buffer_per_file = int(args.bufferPerFile)
    buffer_size = int(args.bufferSize)

    current_file = 0
    counter = 0
    last_timestamp = time.perf_counter_ns()
    counter_last = 0

    while True:
        # Attempt to open stream using pyarrow.ipc.open_stream
        try:
            with open(os.path.join(f'{filename}.{current_file}.arrow'), "rb") as f:
                print(f'Reading {filename}.{current_file}.arrow')
                reader = ipc.open_stream(f)


                for batch in reader:
                    # Access data using batch.column(column_name)
                    expected = generate_batch(schema_by_name(schema_name), buffer_size, counter)
                    for index, field in enumerate(schema_by_name(schema_name)):
                        assert expected[index] == batch[index]

                    counter += buffer_size
                    now = time.perf_counter_ns()

                    if now - last_timestamp > 5 * 1000 * 1000 * 1000:
                        print(f'TPS: {1000 * 1000 * 1000 * ((counter - counter_last) / (now - last_timestamp))}')
                        last_timestamp = now
                        counter_last = counter

                os.remove(f'{filename}.{current_file}.arrow')
                current_file += 1

        except FileNotFoundError:
            # Wait for a short interval before checking again
            time.sleep(0.1)




if __name__ == "__main__":
    main()

# 1052878
# 2641413
