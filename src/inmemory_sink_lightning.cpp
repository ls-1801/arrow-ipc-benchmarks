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
#include <cassert>

#include "../inc/store.h"
#include "../inc/client.h"
#include <iostream>
#include <thread>
#include <cstring>
#include <argparse/argparse.hpp>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include "common.hpp"
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <arrow/ipc/reader.h>

const char* name = "lightning";

struct InMemorySource : public argparse::Args {
    std::string& socket_path = arg("file path");
    size_t& queue_size = arg("shared memory queue size");
    size_t& tuples_per_buffer = arg("number of tuples per buffer");
};


void writeRecordBatchToTupleBuffer(const ArrowFormat& format,
                                   Runtime::TupleBuffer& buffer,
                                   std::shared_ptr<arrow::RecordBatch> recordBatch) {
    size_t tupleCount = buffer.getNumberOfTuples();
    auto schema = buffer.getSchema();
    const auto& fields = schema->fields;
    uint64_t numberOfSchemaFields = schema->fields.size();
    for (uint64_t columnItr = 0; columnItr < numberOfSchemaFields; columnItr++) {
        // retrieve the arrow column to write from the recordBatch
        auto arrowColumn = recordBatch->column(columnItr);

        // write the column to the tuple buffer
        format.writeArrowArrayToTupleBuffer(tupleCount, columnItr, buffer, arrowColumn);
    }
}

using Queue = folly::DynamicBoundedQueue<std::shared_ptr<arrow::RecordBatch>, true, true, true>;

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    auto args = argparse::parse<InMemorySource>(argc, argv);
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto arrow_format = ArrowFormat(schema);
    auto arrow_schema = arrow_format.getArrowSchema();
    LightningClient client(args.socket_path, "password");

    size_t max_buffers = args.queue_size;
    size_t current_object_id = 0;

    size_t current = 0;
    size_t last = 0;
    auto queue = Queue(100);

    auto last_timestamp = std::chrono::high_resolution_clock::now();
    auto interval = std::chrono::seconds(5);
    auto consumer = std::jthread([&](const std::stop_token& stoken) {
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

    while (true) {
        uint8_t* ptr;
        size_t length;
        client.Subscribe(current_object_id % max_buffers);
        client.Get(current_object_id % max_buffers, &ptr, &length);
        // std::cout << "Read " << current_object_id << "\n";
        auto buffer = arrow::Buffer::Wrap(ptr, length);
        auto reader = arrow::io::BufferReader(buffer);
        auto batch_reader =
                arrow::ipc::RecordBatchStreamReader::Open(&reader).ValueOrDie();
        std::shared_ptr<arrow::RecordBatch> batch;
        batch_reader->ReadNext(&batch);
        client.Delete(current_object_id % max_buffers);
        queue.enqueue(batch);
        current_object_id++;
    }

    return 0;
}
