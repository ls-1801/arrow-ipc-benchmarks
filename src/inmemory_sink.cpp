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

#include <iostream>
#include <thread>
#include <cstring>
#include <argparse/argparse.hpp>
#include <arrow/buffer.h>
#include "memoryipc.hpp"
#include <arrow/io/memory.h>
#include "common.hpp"
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <arrow/ipc/reader.h>

const char *name = "lightning";

struct InMemorySource : public argparse::Args {
    std::string &shared_memory_name = arg("shared memory name");
    size_t &queue_size = arg("producer consumer queue size");
    size_t &tuples_per_buffer = arg("number of tuples per buffer");
    size_t &buffers_to_consume = arg("number of buffers that will be received before terminating");
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

using SPSCQueue = folly::DynamicBoundedQueue<DescriptorView, true, true, true>;

int main(int argc, char **argv) {
    using namespace std::chrono_literals;
    auto args = argparse::parse<InMemorySource>(argc, argv);
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto arrow_format = ArrowFormat(schema);
    auto arrow_schema = arrow_format.getArrowSchema();

    size_t current = 0;
    size_t last = 0;
    auto queue = SPSCQueue(100);

    auto last_timestamp = std::chrono::high_resolution_clock::now();
    auto interval = std::chrono::seconds(5);
    SinkQueue memory_queue(args.shared_memory_name);

    auto consumer = std::jthread([&](const std::stop_token &stoken) {
        using namespace std::chrono_literals;
        while (!stoken.stop_requested()) {
            if (DescriptorView buffer{}; queue.try_dequeue_for(buffer, 1s)) {
                const auto arrow_buffer = buffer.as_buffer();
                auto reader = arrow::io::BufferReader(arrow_buffer);
                const auto batch_reader = arrow::ipc::RecordBatchStreamReader::Open(&reader).ValueOrDie();
                std::shared_ptr<arrow::RecordBatch> batch;
                if (auto status = batch_reader->ReadNext(&batch); !status.ok()) {
                    std::cerr << "Error :" << status << std::endl;
                    exit(1);
                }

                Runtime::TupleBuffer tb(batch->num_rows(), schema);
                writeRecordBatchToTupleBuffer(arrow_format, tb, batch);
                memory_queue.free_buffer(std::move(buffer));
                tb.setNumberOfTuples(batch->num_rows());

                for (auto tuple: tb) {
                    assert(tuple[0].read<int64_t>() == current && "Does not match");
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

    for (size_t i = 0; i < args.buffers_to_consume; i++) {
        std::optional<DescriptorView> buffer;
        while (!(buffer = memory_queue.read_buffer()).has_value()) {
            std::this_thread::sleep_for(10ms);
        }
        queue.enqueue(*buffer);
    }

    return 0;
}
