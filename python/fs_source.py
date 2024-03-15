import argparse
import os
from common import *

import numpy as np
import pyarrow
import pyarrow.ipc as ipc


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

    filename = args.filename
    schema_name = args.schemaName
    buffer_per_file = int(args.bufferPerFile)
    buffer_size = int(args.bufferSize)

    current_file = 0
    counter = 0

    while True:
        with open(os.path.join(f'{filename}.{current_file}'), "wb") as f:
            print(f'Writing {filename}.{current_file}.arrow\n')
            writer = pyarrow.RecordBatchStreamWriter(f, schema=schema_by_name(schema_name))

            for _ in range(buffer_per_file):
                batch = pyarrow.RecordBatch.from_arrays(generate_batch(schema_by_name(schema_name), buffer_size, counter), schema=schema_by_name(schema_name))
                counter += buffer_size
                writer.write(batch)

            writer.close()
            os.rename(f'{filename}.{current_file}', f'{filename}.{current_file}.arrow')
            current_file += 1


if __name__ == "__main__":
    main()

# 1052878
# 2641413
