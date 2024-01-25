/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */

#include <arrow/flight/api.h>
#include <iostream>
#include "common.hpp"
#include <arrow/ipc/api.h>
#include <argparse/argparse.hpp>
#include <folly/concurrency/DynamicBoundedQueue.h>

struct SocketSourceArgs : public argparse::Args {
    short& port = arg("Port number");
    size_t& tuples_per_buffer = arg("number of tuples per buffer");
};

std::string create_file_name(const std::string& filename_template, size_t file_number) {
    return fmt::format("{}.{}.arrow", filename_template, file_number);
}

arrow::Status write_tuples_to_file(const std::string& filename_template,
                                   const std::function<std::optional<Runtime::TupleBuffer>()>& supplier,
                                   size_t tuples_per_file,
                                   const std::shared_ptr<arrow::Schema>& arrow_schema,
                                   const ArrowFormat& arrow_format
) {
    size_t file_number = 0;
    std::shared_ptr<arrow::io::FileOutputStream> outfileArrow;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
    auto tb = *supplier();
    auto arrow_arrays = arrow_format.getArrowArrays(tb);
    std::shared_ptr<arrow::RecordBatch> recordBatch =
            arrow::RecordBatch::Make(arrow_schema, arrow_arrays[0]->length(), arrow_arrays);
open_file:
    size_t tuples_written = 0;
    NES_WARNING("Creating new file {}\n", create_file_name(filename_template, file_number));
    ARROW_ASSIGN_OR_RAISE(outfileArrow,
                          arrow::io::FileOutputStream::Open(create_file_name(filename_template, file_number)));
    ARROW_ASSIGN_OR_RAISE(arrowWriter, arrow::ipc::MakeStreamWriter(outfileArrow,arrow_schema));
new_buffer:

    // write the record batch
    auto write = arrowWriter->WriteRecordBatch(*recordBatch);
    tuples_written += tb.getNumberOfTuples();

    if (auto tb_opt = supplier()) {
        tb = *tb_opt;
        arrow_arrays = arrow_format.getArrowArrays(tb);
        recordBatch = arrow::RecordBatch::Make(arrow_schema, arrow_arrays[0]->length(), arrow_arrays);
    } else {
        return arrow::Status::OK();
    }

    if (tuples_written + tb.getNumberOfTuples() > tuples_per_file) {
        file_number++;
        goto open_file;
    }

    goto new_buffer;
}

using Queue = folly::DynamicBoundedQueue<Runtime::TupleBuffer, true, true, true>;

arrow::Status arrow_main(const SocketSourceArgs& args) {
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));

    auto format = std::make_shared<ArrowFormat>(schema);
    auto arrow_schema = format->getArrowSchema();

    auto queue = Queue(100);

    auto producer = std::jthread([args, &schema, &queue](const std::stop_token& stoken) {
        using namespace std::chrono_literals;
        int64_t i = 0;
        while (!stoken.stop_requested()) {
            auto tb = Runtime::TupleBuffer(args.tuples_per_buffer, schema);
            tb.setNumberOfTuples(args.tuples_per_buffer);
            for (auto tuple: tb) {
                tuple[0].write<int64_t>(i++);
            }
            // loop as long as enqueue does not work and stop stop is not requested
            while (!queue.try_enqueue_for(std::move(tb), 100ms) && !stoken.stop_requested()) {
            }
        }
    });

    arrow::flight::Location location;
    ARROW_RETURN_NOT_OK(arrow::flight::Location::ForGrpcTcp("localhost", args.port, &location));

    std::unique_ptr<arrow::flight::FlightClient> client;
    ARROW_RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));

    std::cout << "Connected to " << location.ToString() << std::endl;

    while (true) {
        Runtime::TupleBuffer tb(0, schema);
        queue.dequeue(tb);
        auto arrow_arrays = format->getArrowArrays(tb);
        auto recordBatch = arrow::RecordBatch::Make(arrow_schema, arrow_arrays[0]->length(), arrow_arrays);

        auto descriptor = arrow::flight::FlightDescriptor::Path({"airquality.parquet"});

        // Start the RPC call
        std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
        std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
        ARROW_RETURN_NOT_OK(client->DoPut(descriptor, arrow_schema, &writer, &metadata_reader));

        // Upload data
        std::shared_ptr<arrow::RecordBatchReader> batch_reader;
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
    }

    return arrow::Status::OK();
}

int main(int argc, char* argv[]) {
    auto args = argparse::parse<SocketSourceArgs>(argc, argv);

    std::cerr << arrow_main(args).message() << std::endl;

    return 0;
}
