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
#include "../inc/store.h"
#include "../inc/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <argparse/argparse.hpp>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/writer.h>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include "common.hpp"
#include <folly/concurrency/DynamicBoundedQueue.h>
#include "memoryipc.hpp"

const char* name = "lightning";

struct InMemorySource : public argparse::Args {
    std::string& shared_memory_name = arg("shared memory name");
    size_t& queue_size = arg("number of buffer per file");
    size_t& tuples_per_buffer = arg("number of tuples per buffer");
    size_t& number_of_buffers_to_produce = arg("number of buffers to produce");
    size_t& memory_descriptor_queue_size = arg("size of the shared memory descriptor queue");
    size_t& shared_memory_region_size = arg("size of the the shared memory region");
};

using SPSCQueue = folly::DynamicBoundedQueue<Runtime::TupleBuffer, true, true, true>;


int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    auto args = argparse::parse<InMemorySource>(argc, argv);
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto arrow_format = ArrowFormat(schema);
    auto arrow_schema = arrow_format.getArrowSchema();

    SourceQueue memory_queue(args.shared_memory_name, args.memory_descriptor_queue_size,
                             args.shared_memory_region_size);
    SPSCQueue producer_queue{100};

    auto producer = std::jthread([&producer_queue, &schema, args](const std::stop_token& stoken) {
        int64_t current = 0;
        while (!stoken.stop_requested()) {
            auto tb = Runtime::TupleBuffer(args.tuples_per_buffer, schema);
            tb.setNumberOfTuples(1024);
            for (auto tuple: tb) {
                tuple[0].write<int64_t>(current++);
            }
            while (!stoken.stop_requested() && !producer_queue.try_enqueue_for(tb, 1s)) {
                std::cerr << "Producer queue is full";
            }
        }
    });

    for (size_t i = 0; i < args.number_of_buffers_to_produce; i++) {
        Runtime::TupleBuffer tb(0, schema);
        producer_queue.dequeue(tb);
        auto arrays = arrow_format.getArrowArrays(tb);
        auto recordBatch = arrow::RecordBatch::Make(arrow_schema, arrays[0]->length(), arrays);

        int64_t batch_size = 0;
        arrow::ipc::GetRecordBatchSize(*recordBatch, &batch_size);
        batch_size += 1000;

        auto descritpor = memory_queue.allocate_buffer(batch_size);
        auto writer = arrow::io::FixedSizeBufferWriter(descritpor.as_mutable_buffer());
        auto stream_writer = arrow::ipc::MakeStreamWriter(&writer, arrow_schema).ValueOrDie();
        if (auto status = stream_writer->WriteRecordBatch(*recordBatch); !status.ok()) {
            std::cerr << "Writing batch failed: " << status << std::endl;
            exit(1);
        }

        auto timeout = 10ms;
        std::optional<DescriptorView> opt_desc;
        while ((opt_desc = memory_queue.write_buffer(std::move(descritpor))).has_value()) {
            descritpor = *opt_desc;
            std::this_thread::sleep_for(timeout);
        }
    }
    return 0;
}


// Conversion in Main Thread:
// TPS:   34001305
// TPS:   41029632
// TPS:   33444044
// TPS:   34145894
// TPS:   48394240

// Conversion in Consumer Thread
// TPS:   58777395
// TPS:   44520652
// TPS:   44200960
// TPS:   37852979
// TPS:   19312435
