from common import *
import argparse
import pathlib
import sys
import time
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet


class FlightServer(pa.flight.FlightServerBase):

    def __init__(self, schema_name, buffer_size, location="grpc://0.0.0.0:8080", **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self.counter = 0
        self.buffer_size = buffer_size
        self.last_timestamp = time.perf_counter_ns()
        self.counter_last = 0
        self.schema = schema_by_name(schema_name)

    def _make_flight_info(self, dataset):
        raise "NotImplemented yet"

    def list_flights(self, context, criteria):
        raise "NotImplemented yet"

    def get_flight_info(self, context, descriptor):
        raise "NotImplemented yet"

    def do_put(self, context, descriptor, reader, writer):
        batch_reader = reader.to_reader()
        # batch_reader = pyarrow.RecordBatchReader(reader)
        for batch in batch_reader:

            expected = generate_batch(self.schema, self.buffer_size, self.counter)
            # for index, field in enumerate(self.schema):
            #     assert expected[index] == batch[index]

            self.counter += self.buffer_size
            now = time.perf_counter_ns()

            if now - self.last_timestamp > 2 * 1000 * 1000 * 1000:
                print(f'TPS: {1000 * 1000 * 1000 * ((self.counter - self.counter_last) / (now - self.last_timestamp))}')
                self.last_timestamp = now
                self.counter_last = self.counter


    def do_get(self, context, ticket):
        raise "NotImplemented yet"

    def list_actions(self, context):
        raise "NotImplemented yet"

    def do_action(self, context, action):
        raise "NotImplemented yet"

    def do_drop_dataset(self, dataset):
        raise "NotImplemented yet"


def main():
    """Continuously monitors for new Arrow IPC files and reads them."""

    parser = argparse.ArgumentParser(
        prog='socket_sink.py'
    )

    parser.add_argument('schemaName')
    parser.add_argument('port')
    parser.add_argument('bufferPerRequest')
    parser.add_argument('bufferSize')

    args = parser.parse_args()

    schema_name = args.schemaName.lower()
    port = int(args.port)
    buffer_size = int(args.bufferSize)

    server = FlightServer(schema_name, buffer_size)
    server.serve()
    print("here")

def trace(frame, event, arg):
    print("%s, %s:%d" % (event, frame.f_code.co_filename, frame.f_lineno))
    return trace


# sys.settrace(trace)
if __name__ == '__main__':
    main()