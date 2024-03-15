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
#include <arrow/flight/server.h>

struct FSSourceArgs : public argparse::Args {
    std::string &schemaName = arg("Schema");
    short &port = arg("Port number");
    size_t &buffer_per_request = arg("number of buffer per request");
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

using Queue = folly::DynamicBoundedQueue<std::shared_ptr<arrow::RecordBatch>, true, true, true>;

class SinkService : public arrow::flight::FlightServerBase {
    Queue &queue;

public:
    explicit SinkService(Queue &queue)
        : queue(queue) {
    }

    arrow::Status DoPut(const arrow::flight::ServerCallContext &context,
                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_ASSIGN_OR_RAISE(auto batch_reader, arrow::flight::MakeRecordBatchReader(std::move(reader)));
        ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&batch));
        while (batch) {
            queue.enqueue(batch);
            ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&batch));
        }
        return arrow::Status::OK();
    }

    ~SinkService() override = default;

    arrow::Status ListFlights(const arrow::flight::ServerCallContext &context, const arrow::flight::Criteria *criteria,
                              std::unique_ptr<arrow::flight::FlightListing> *listings) override {
        NES_NOT_IMPLEMENTED();
    }

    arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext &context,
                                const arrow::flight::FlightDescriptor &request,
                                std::unique_ptr<arrow::flight::FlightInfo> *info) override {
        NES_NOT_IMPLEMENTED();
    }

    arrow::Status GetSchema(const arrow::flight::ServerCallContext &context,
                            const arrow::flight::FlightDescriptor &request,
                            std::unique_ptr<arrow::flight::SchemaResult> *schema) override {
        NES_NOT_IMPLEMENTED();
    }

    arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &request,
                        std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {
        NES_NOT_IMPLEMENTED();
    }

    arrow::Status DoExchange(const arrow::flight::ServerCallContext &context,
                             std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                             std::unique_ptr<arrow::flight::FlightMessageWriter> writer) override {
        NES_NOT_IMPLEMENTED();
    }

    arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
                           std::unique_ptr<arrow::flight::ResultStream> *result) override {
        NES_NOT_IMPLEMENTED();
    }

    arrow::Status ListActions(const arrow::flight::ServerCallContext &context,
                              std::vector<arrow::flight::ActionType> *actions) override {
        NES_NOT_IMPLEMENTED();
    }
};

arrow::Status arrow_main(const FSSourceArgs &args) {
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto arrow_format = ArrowFormat(schema);
    size_t current = 0;
    size_t last = 0;
    auto queue = Queue(100);

    auto last_timestamp = std::chrono::high_resolution_clock::now();
    auto interval = std::chrono::seconds(2);


    auto consumer = std::jthread([&](const std::stop_token &stoken) {
        using namespace std::chrono_literals;
        while (!stoken.stop_requested()) {
            if (std::shared_ptr<arrow::RecordBatch> batch; queue.try_dequeue_for(batch, 1s)) {
                auto expected = generate_batch(schema_by_name(args.schemaName), args.tuples_per_buffer, current);

                if (!expected->Equals(*batch)) {
                    if (expected->num_columns() != batch->num_columns()) {
                        NES_ERROR("Cols do not machts");
                    }

                    for (size_t index = 0; const auto &col: expected->columns()) {
                        if (!col->Equals(batch->columns()[index])) {
                            // NES_ERROR("Not the same: {}", index);
                            // arrow::PrettyPrint(*col, 0, &std::cout);
                            // arrow::PrettyPrint(*batch->columns()[index], 0, &std::cout);
                        }
                        index++;
                        // exit(0);
                    }
                    // arrow::PrettyPrint(*expected, 0, &std::cout);
                    // arrow::PrettyPrint(*batch, 0, &std::cout);
                    // NES_ERROR("Not the same");
                    // exit(0);
                }

                current += args.tuples_per_buffer;

                auto now = std::chrono::high_resolution_clock::now();
                if (last_timestamp + interval <= now) {
                    NES_WARNING("TPS: {:10L}\n", (current - last) / interval.count());
                    last_timestamp = now;
                    last = current;
                }
            }
        }
    });


    arrow::flight::Location server_location;
    ARROW_RETURN_NOT_OK(arrow::flight::Location::ForGrpcTcp("0.0.0.0", args.port, &server_location));

    arrow::flight::FlightServerOptions options(server_location);
    auto serivce = SinkService(queue);
    ARROW_RETURN_NOT_OK(serivce.Init(options));
    std::cout << "Listening on port " << serivce.port() << std::endl;

    serivce.Wait();

    return arrow::Status::OK();
}

int main(int argc, char *argv[]) {
    auto args = argparse::parse<FSSourceArgs>(argc, argv);

    arrow_main(args);

    return 0;
}
