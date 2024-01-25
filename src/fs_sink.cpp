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
                                          const SchemaPtr &schema,
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
            if (reader_result.ok()) {
                reader = reader_result.ValueOrDie();
                NES_TRACE("Successfully opened file\n");
                break;
            }
        }
        NES_WARNING("\tFile does not exist!\n");
        std::this_thread::sleep_for(retry_interval);
        retry_interval *= 2;
    } while (true);

new_batch:
    std::shared_ptr<arrow::RecordBatch> batch;
    auto batch_retry_interval = 50ms;
    do {
        NES_TRACE("Reading next batch\n");
        reader->ReadNext(&batch);
        if (batch) {
            index_within_file += batch->num_rows();
            NES_TRACE("Succesfully read next batch\n");
            break;
        }
        NES_TRACE("\tReached EOF\n");
        if (index_within_file < tuples_per_file) {
            if (batch_retry_interval > 1s) {
                NES_WARNING("Retry Intervall longer than 1s");
                return arrow::Status::UnknownError("Retry intervall execeeds limit");
            }

            NES_WARNING("\tExpecting more tuples {}%\n",
                        static_cast<double>(index_within_file) / static_cast<double>(tuples_per_file) * 100.00);
            std::this_thread::sleep_for(batch_retry_interval);
            batch_retry_interval *= 2;
        } else {
            std::filesystem::remove(create_file_name(filename_template, file_number));
            file_number++;
            goto open_file;
        }
    } while (true);
    size_t index_within_current_batch = 0;
new_buffer:
    const size_t tuple_size = std::min(tuple_buffer_size, batch->num_rows() - index_within_current_batch);
    emitter(batch->Slice(index_within_current_batch, tuple_size));
    index_within_current_batch += tuple_size;
    if (index_within_current_batch == batch->num_rows()) {
        goto new_batch;
    }
    goto new_buffer;
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
                Runtime::TupleBuffer tb(batch->num_rows(), schema);
                writeRecordBatchToTupleBuffer(arrow_format, tb, batch);
                tb.setNumberOfTuples(batch->num_rows());
                for (auto tuple: tb) {
                    if (current != tuple[0].read<int64_t>()) {
                        NES_TRACE("Incorrect Tuple Value: Expected {} got {}\n", current,
                                  tuple[0].read<int64_t>());
                    }
                    current++;
                }
                auto now = std::chrono::high_resolution_clock::now();
                if (last_timestamp + interval <= now) {
                    NES_WARNING("TPS: {:10L}\n", (current - last) / interval.count());
                    last_timestamp = now;
                    last = current;
                }
            }
        }
    });

    read_tuplebuffers_from_file(args.file_name, schema, args.tuples_per_buffer * args.buffer_per_file,
                                args.tuples_per_buffer,
                                [&](std::shared_ptr<arrow::RecordBatch> &&record_batch) {
                                    queue.enqueue(record_batch);
                                });
    return arrow::Status::OK();
}

int main(int argc, char *argv[]) {
    auto args = argparse::parse<FSSourceArgs>(argc, argv);

    arrow_main(args);

    return 0;
}
