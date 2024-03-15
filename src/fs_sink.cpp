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

#include <chrono>
#include <iostream>
#include <thread>

#include "common.hpp"
#include <arrow/ipc/api.h>
#include <argparse/argparse.hpp>
#include <folly/concurrency/DynamicBoundedQueue.h>

struct FSSourceArgs : public argparse::Args {
    std::string &schemaName = arg("Schema");
    std::string &file_name = arg("file path");
    size_t &buffer_per_file = arg("number of buffer per file");
    size_t &tuples_per_buffer = arg("number of tuples per buffer");
};

void writeRecordBatchToTupleBuffer(const ArrowFormat &format,
                                   Runtime::TupleBuffer &buffer,
                                   std::shared_ptr<arrow::RecordBatch> recordBatch) {
    size_t tupleCount = buffer.getNumberOfTuples();
    auto schema = buffer.getSchema();
    const auto &fields = schema->fields;
    uint64_t numberOfSchemaFields = schema->fields.size();
    for (uint64_t columnItr = 0; columnItr < numberOfSchemaFields; columnItr++) {
        // retrieve the arrow column to write from the recordBatch
        auto arrowColumn = recordBatch->column(columnItr);

        // write the column to the tuple buffer
        format.writeArrowArrayToTupleBuffer(tupleCount, columnItr, buffer, arrowColumn);
    }
}

std::string create_file_name(const std::string &filename_template, size_t file_number) {
    return fmt::format("{}.{}.arrow", filename_template, file_number);
}

arrow::Status read_tuplebuffers_from_file(const std::string &filename_template,
                                          size_t tuples_per_file,
                                          size_t tuple_buffer_size,
                                          std::function<void(std::shared_ptr<arrow::RecordBatch> &&)> emitter
) {
    using namespace std::chrono_literals;
    size_t file_number = 0;
open_file:
    std::shared_ptr<arrow::io::ReadableFile> inputFile;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader;
    size_t index_within_file = 0;
    auto retry_interval = 50ms;
    do {
        auto open_result = arrow::io::ReadableFile::Open(create_file_name(filename_template, file_number));
        NES_WARNING("Opening file: {}\n", create_file_name(filename_template, file_number));
        if (open_result.ok()) {
            inputFile = open_result.ValueOrDie();
            auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(inputFile);
            reader = reader_result.ValueOrDie();
            NES_TRACE("Successfully opened file\n");
            break;
        }
        NES_WARNING("\tFile does not exist!\n");
        std::this_thread::sleep_for(retry_interval);
        retry_interval *= 2;
    } while (true);

new_batch:
    std::shared_ptr<arrow::RecordBatch> batch;
    NES_TRACE("Reading next batch\n");
    reader->ReadNext(&batch);
    if (batch) {
        index_within_file += batch->num_rows();
        emitter(std::move(batch));
        goto new_batch;
    }

    NES_TRACE("\tReached EOF\n");
    if (index_within_file < tuples_per_file) {
        NES_ERROR("\tExpecting more tuples {}%\n",
                  static_cast<double>(index_within_file) / static_cast<double>(tuples_per_file) * 100.00);
        exit(0);
    } else {
        std::filesystem::remove(create_file_name(filename_template, file_number));
        file_number++;
        goto open_file;
    }
}

using Queue = folly::DynamicBoundedQueue<std::shared_ptr<arrow::RecordBatch>, true, true, true>;

arrow::Status arrow_main(const FSSourceArgs &args) {
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto arrow_format = ArrowFormat(schema);
    size_t current = 0;
    size_t last = 0;
    auto queue = Queue(100);

    auto last_timestamp = std::chrono::high_resolution_clock::now();
    auto interval = std::chrono::seconds(5);


    auto consumer = std::jthread([&](const std::stop_token &stoken) {
        using namespace std::chrono_literals;
        while (!stoken.stop_requested()) {
            if (std::shared_ptr<arrow::RecordBatch> batch; queue.try_dequeue_for(batch, 1s)) {
                auto expected = generate_batch(schema_by_name(args.schemaName), batch->num_rows(), current);

                if (!expected->Equals(*batch)) {
                    arrow::PrettyPrint(*expected, 0, &std::cout);
                    arrow::PrettyPrint(*batch, 0, &std::cout);
                    NES_ERROR("Not the same");
                    exit(0);
                }

                current += batch->num_rows();

                auto now = std::chrono::high_resolution_clock::now();
                if (last_timestamp + interval <= now) {
                    NES_WARNING("TPS: {:10L}\n", (current - last) / interval.count());
                    last_timestamp = now;
                    last = current;
                }
            }
        }
    });

    read_tuplebuffers_from_file(args.file_name, args.tuples_per_buffer * args.buffer_per_file,
                                args.tuples_per_buffer,
                                [&](std::shared_ptr<arrow::RecordBatch> &&record_batch) {
                                    queue.enqueue(record_batch);
                                });
    return arrow::Status::OK();
}

int main(int argc, char *argv[]) {
    // std::this_thread::sleep_for(std::chrono::seconds(20));
    auto args = argparse::parse<FSSourceArgs>(argc, argv);

    arrow_main(args);

    return 0;
}
