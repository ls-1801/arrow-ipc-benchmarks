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

struct FSSourceArgs : public argparse::Args {
    std::string& file_name = arg("file path");
};

arrow::Status arrow_main(const FSSourceArgs& args) {
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto tb = Runtime::TupleBuffer(10, schema);
    tb.setNumberOfTuples(10);
    auto format = std::make_shared<ArrowFormat>(schema);
    auto arrow_schema = format->getArrowSchema();

    for (auto tuple: tb) {
        tuple["id"].write<int64_t>(3);
    }
    std::shared_ptr<arrow::io::FileOutputStream> outfileArrow;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
    ARROW_ASSIGN_OR_RAISE(outfileArrow, arrow::io::FileOutputStream::Open(args.file_name));
    ARROW_ASSIGN_OR_RAISE(arrowWriter, arrow::ipc::MakeStreamWriter(outfileArrow, arrow_schema));
    auto arrow_arrays = format->getArrowArrays(tb);

    std::shared_ptr<arrow::RecordBatch> recordBatch =
            arrow::RecordBatch::Make(arrow_schema, arrow_arrays[0]->length(), arrow_arrays);

    // write the record batch
    auto write = arrowWriter->WriteRecordBatch(*recordBatch);

    auto arrays = format->getArrowArrays(tb);

    return arrow::Status::OK();
}

int main(int argc, char* argv[]) {
    auto args = argparse::parse<FSSourceArgs>(argc, argv);

    arrow_main(args);

    return 0;
}
