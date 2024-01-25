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
#include <cstring>
#include <chrono>
#include <argparse/argparse.hpp>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/writer.h>

#include "common.hpp"
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <bits/ranges_algo.h>

const char* name = "lightning";

struct InMemorySource : public argparse::Args {
    std::string& socket_path = arg("file path");
    size_t& queue_size = arg("number of buffer per file");
    size_t& tuples_per_buffer = arg("number of tuples per buffer");
};

using Queue = folly::DynamicBoundedQueue<Runtime::TupleBuffer, true, true, true>;

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    auto args = argparse::parse<InMemorySource>(argc, argv);
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto arrow_format = ArrowFormat(schema);
    auto arrow_schema = arrow_format.getArrowSchema();

    auto store_thread = std::jthread([args]() {
        LightningStore store(args.socket_path, 1024l * 1024l * 1024l);
        store.Run();
    });

    std::this_thread::sleep_for(1s);
    LightningClient client(args.socket_path, "password");

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

    size_t max_number_of_buffers = args.queue_size;
    size_t current_object_id = 0;
    uint8_t* ptr;

    while (true) {
        auto interval_timeout = 100ms;
        auto tb = Runtime::TupleBuffer(0, schema);
        queue.dequeue(tb);
        auto arrow_arrays = arrow_format.getArrowArrays(tb);
        auto recordBatch = arrow::RecordBatch::Make(arrow_schema, arrow_arrays[0]->length(), arrow_arrays);
        int64_t batch_size_in_bytes = 0;
        arrow::ipc::GetRecordBatchSize(*recordBatch, &batch_size_in_bytes);

        while (client.Create(current_object_id % max_number_of_buffers, &ptr, batch_size_in_bytes + 1000) == -1) {
            std::cout << current_object_id % max_number_of_buffers << "still exists" << std::endl;
            client.Seal(current_object_id % max_number_of_buffers);
            std::this_thread::sleep_for(interval_timeout);
            interval_timeout *= 2;
        }

        auto buffer = arrow::MutableBuffer::Wrap(ptr, batch_size_in_bytes + 1000);
        auto writer = arrow::io::FixedSizeBufferWriter(buffer);
        auto stream_writer = arrow::ipc::MakeStreamWriter(&writer, arrow_schema).ValueOrDie();
        auto status = stream_writer->WriteRecordBatch(*recordBatch);
        if (!status.ok()) {
            std::cerr << "Writing batch failed: " << status << std::endl;
            exit(1);
        }
        client.Seal(current_object_id % max_number_of_buffers);
        current_object_id++;
    }

    return 0;
}
