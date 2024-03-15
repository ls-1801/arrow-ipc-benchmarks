from common import *
import argparse
import pathlib
import time
import pyarrow as pa
import numpy as np
import pyarrow.flight
import pyarrow.parquet

def main():
    """Continuously monitors for new Arrow IPC files and reads them."""
    time.sleep(1)
    parser = argparse.ArgumentParser(
        prog='socket_source.py'
    )

    parser.add_argument('schemaName')
    parser.add_argument('port')
    parser.add_argument('bufferPerRequest')
    parser.add_argument('bufferSize')

    args = parser.parse_args()

    schema_name = args.schemaName.lower()
    port = int(args.port)
    buffer_size = int(args.bufferSize)
    buffer_per_request = int(args.bufferPerRequest)

    counter = 0
    print(f'port: {port}, bufferSize: {buffer_size}, bufferPerRequest: {buffer_per_request}\n')
    client = pyarrow.flight.connect(f'grpc://localhost:{port}', generic_options=[("grpc.max_concurrent_streams", 1)])

    while True:
        descriptor = pyarrow.flight.FlightDescriptor.for_path("airquality.parquet")

        schema = schema_by_name(schema_name)
        writer, _ = client.do_put(descriptor, schema)
        for _ in range(buffer_per_request):
            data = generate_batch(schema, buffer_size, counter)
            counter += buffer_size
            batch = pyarrow.RecordBatch.from_arrays(data, schema=schema)
            writer.write(batch)

        writer.close()


if __name__ == '__main__':
    main()