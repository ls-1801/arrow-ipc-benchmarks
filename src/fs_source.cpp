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

#include <iostream>
#include "common.hpp"
#include <arrow/ipc/api.h>
#include <argparse/argparse.hpp>
#include <functional>
#include <folly/concurrency/DynamicBoundedQueue.h>

struct FSSourceArgs : public argparse::Args {
    std::string &schemaName = arg("Schema");
    std::string &file_name = arg("file path");
    size_t &buffer_per_file = arg("number of buffer per file");
    size_t &tuples_per_buffer = arg("number of tuples per buffer");
};

std::string create_file_name(const std::string &filename_template, size_t file_number) {
    return fmt::format("{}.{}", filename_template, file_number);
}

arrow::Status write_tuples_to_file(const std::string &filename_template,
                                   const std::function<std::shared_ptr<arrow::RecordBatch>()> &supplier,
                                   size_t tuples_per_file,
                                   const std::shared_ptr<arrow::Schema> &arrow_schema,
                                   const ArrowFormat &arrow_format
) {
    size_t file_number = 0;
    std::shared_ptr<arrow::io::FileOutputStream> outfileArrow;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
    auto recordBatch = supplier();
open_file:
    size_t tuples_written = 0;
    // NES_WARNING("Creating new file {}\n", create_file_name(filename_template, file_number));
    ARROW_ASSIGN_OR_RAISE(outfileArrow,
                          arrow::io::FileOutputStream::Open(create_file_name(filename_template, file_number)));
    ARROW_ASSIGN_OR_RAISE(arrowWriter, arrow::ipc::MakeStreamWriter(outfileArrow,arrow_schema));
new_buffer:

    // PrettyPrint(*recordBatch, 8, &std::cout);
    // write the record batch
    ARROW_RETURN_NOT_OK(arrowWriter->WriteRecordBatch(*recordBatch));
    tuples_written += recordBatch->num_rows();

    if (auto tb_opt = supplier()) {
        recordBatch = std::move(tb_opt);
    } else {
        return arrow::Status::OK();
    }

    if (tuples_written + recordBatch->num_rows() > tuples_per_file) {
        std::rename(create_file_name(filename_template, file_number).c_str(),
                    fmt::format("{}.arrow", create_file_name(filename_template, file_number)).c_str());
        file_number++;
        goto open_file;
    }

    goto new_buffer;
}

using Queue = folly::DynamicBoundedQueue<std::shared_ptr<arrow::RecordBatch>, true, true, true>;

arrow::Status arrow_main(const FSSourceArgs &args) {
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));

    auto format = std::make_shared<ArrowFormat>(schema);
    auto arrow_schema = format->getArrowSchema();

    auto queue = Queue(100);
    auto producer = std::jthread([args, &schema, &queue](const std::stop_token &stoken) {
        using namespace std::chrono_literals;
        int64_t i = 0;
        while (!stoken.stop_requested()) {
            auto batch = generate_batch(schema_by_name(args.schemaName), args.tuples_per_buffer, i);
            i += args.tuples_per_buffer;
            while (!queue.try_enqueue_for(std::move(batch), 100ms) && !stoken.stop_requested()) {
            }
        }
    });

    int current = 0;
    std::function<std::shared_ptr<arrow::RecordBatch>()> supplier = [=, &queue, &schema, &current]() {
        std::shared_ptr<arrow::RecordBatch> batch;
        queue.dequeue(batch);
        return batch;
    };
    ARROW_RETURN_NOT_OK(
        write_tuples_to_file(args.file_name, supplier, args.buffer_per_file * args.tuples_per_buffer, schema_by_name(
                args.schemaName),
            *format));

    return arrow::Status::OK();
}

int main(int argc, char *argv[]) {
    auto args = argparse::parse<FSSourceArgs>(argc, argv);

    auto result = arrow_main(args);
    if (!result.ok()) {
        NES_ERROR("Arrow Error: {}", result.message());
    }

    return 0;
}
