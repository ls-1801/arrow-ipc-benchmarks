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


class ArrowSource {
public:
    ArrowSource(std::shared_ptr<ArrowFormat> format,
                std::shared_ptr<arrow::ipc::RecordBatchStreamReader> record_batch_stream_reader,
                std::shared_ptr<arrow::io::ReadableFile> input_file)
        : format(std::move(format)),
          recordBatchStreamReader(std::move(record_batch_stream_reader)),
          inputFile(std::move(input_file)) {
    }

private:
    size_t indexWithinCurrentRecordBatch = 0;
    std::shared_ptr<ArrowFormat> format;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> recordBatchStreamReader;
    std::shared_ptr<arrow::RecordBatch> currentRecordBatch;
    std::shared_ptr<arrow::io::ReadableFile> inputFile;
    size_t generatedTuples = 0;
    size_t generatedBuffers = 0;

    bool fileEnded = false;
    int numberOfTuplesToProducePerBuffer = 0;

public:
    void readNextBatch();

    void writeRecordBatchToTupleBuffer(uint64_t tupleCount, Runtime::TupleBuffer& buffer,
                                       std::shared_ptr<arrow::RecordBatch> recordBatch);

    void fillBuffer(Runtime::TupleBuffer& buffer);
};

void ArrowSource::readNextBatch() {
    // set the internal index to 0 and read the new batch
    indexWithinCurrentRecordBatch = 0;
    auto readStatus = arrow::Status::OK();
    readStatus = recordBatchStreamReader->ReadNext(&currentRecordBatch);

    // check if file has ended
    if (currentRecordBatch == nullptr) {
        this->fileEnded = true;
        NES_TRACE("ArrowSource::readNextBatch: file has ended.");
        return;
    }

    // check if there was some error reading the batch
    if (!readStatus.ok()) {
        NES_THROW_RUNTIME_ERROR("ArrowSource::fillBuffer: error reading recordBatch: " << readStatus.ToString());
    }

    NES_TRACE("ArrowSource::readNextBatch: read the following record batch {}", currentRecordBatch->ToString());
}

void ArrowSource::writeRecordBatchToTupleBuffer(uint64_t tupleCount,
                                                Runtime::TupleBuffer& buffer,
                                                std::shared_ptr<arrow::RecordBatch> recordBatch) {
    auto schema = buffer.getSchema();
    auto fields = schema->fields;
    uint64_t numberOfSchemaFields = schema->fields.size();
    for (uint64_t columnItr = 0; columnItr < numberOfSchemaFields; columnItr++) {
        // retrieve the arrow column to write from the recordBatch
        auto arrowColumn = recordBatch->column(columnItr);

        // write the column to the tuple buffer
        format->writeArrowArrayToTupleBuffer(tupleCount, columnItr, buffer, arrowColumn);
    }
}

void ArrowSource::fillBuffer(Runtime::TupleBuffer& buffer) {
    // make sure that we have a batch to read
    if (currentRecordBatch == nullptr) {
        readNextBatch();
    }

    // check if file has not ended
    if (this->fileEnded) {
        NES_WARNING("ArrowSource::fillBuffer: but file has already ended");
        return;
    }

    NES_TRACE("ArrowSource::fillBuffer: start at record_batch={} fileSize={}", currentRecordBatch->ToString(),
              fileSize);

    uint64_t generatedTuplesThisPass = 0;

    // densely pack the buffer
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buffer.getCapacity();
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * buffer.getSchema()->get_tuple_size()<buffer.getBuffer().size(),
                        "ArrowSource::fillBuffer: not enough space in tuple buffer to fill tuples in this pass."
        );
    }
    NES_TRACE("ArrowSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

    uint64_t tupleCount = 0;

    // Compute how many tuples we can generate from the current batch
    uint64_t tuplesRemainingInCurrentBatch = currentRecordBatch->num_rows() - indexWithinCurrentRecordBatch;

    // Case 1. The number of remaining records in the currentRecordBatch are less than generatedTuplesThisPass. Copy the
    // records from the record batch and keep reading and copying new record batches to fill the buffer.
    if (tuplesRemainingInCurrentBatch < generatedTuplesThisPass) {
        // get the slice of the record batch, slicing is a no copy op
        auto recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, tuplesRemainingInCurrentBatch);
        // write the batch to the tuple buffer
        writeRecordBatchToTupleBuffer(tupleCount, buffer, recordBatchSlice);
        tupleCount += tuplesRemainingInCurrentBatch;

        // keep reading record batch and writing to tuple buffer until we have generated generatedTuplesThisPass number
        // of tuples
        while (tupleCount < generatedTuplesThisPass) {
            // read in a new record batch
            readNextBatch();

            // only continue if the file has not ended
            if (this->fileEnded == true)
                break;

            // case when we have to write the whole batch to the tuple buffer
            if ((tupleCount + currentRecordBatch->num_rows()) <= generatedTuplesThisPass) {
                writeRecordBatchToTupleBuffer(tupleCount, buffer, currentRecordBatch);
                tupleCount += currentRecordBatch->num_rows();
                indexWithinCurrentRecordBatch += currentRecordBatch->num_rows();
            }
            // case when we have to write only a part of the batch to the tuple buffer
            else {
                uint64_t lastBatchSize = generatedTuplesThisPass - tupleCount;
                recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, lastBatchSize);
                writeRecordBatchToTupleBuffer(tupleCount, buffer, recordBatchSlice);
                tupleCount += lastBatchSize;
                indexWithinCurrentRecordBatch += lastBatchSize;
            }
        }
    }
    // Case 2. The number of remaining records in the currentRecordBatch are greater than generatedTuplesThisPass.
    // simply copy the desired number of tuples from the recordBatch to the tuple buffer
    else {
        // get the slice of the record batch with desired number of tuples
        auto recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, generatedTuplesThisPass);
        // write the batch to the tuple buffer
        writeRecordBatchToTupleBuffer(tupleCount, buffer, recordBatchSlice);
        tupleCount += generatedTuplesThisPass;
        indexWithinCurrentRecordBatch += generatedTuplesThisPass;
    }

    buffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("ArrowSource::fillBuffer: reading finished read {} tuples", tupleCount);
    NES_TRACE("ArrowSource::fillBuffer: read produced buffer=  {}",
              Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

arrow::Status arrow_main(const FSSourceArgs& args) {
    auto sch = Schema::create();
    auto schema = sch->append(SchemaField::create("id", INT_64));
    auto format = std::make_shared<ArrowFormat>(schema);
    auto arrow_schema = format->getArrowSchema();


    std::shared_ptr<arrow::io::ReadableFile> inputFile;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader;
    ARROW_ASSIGN_OR_RAISE(inputFile, arrow::io::ReadableFile::Open(args.file_name));
    ARROW_ASSIGN_OR_RAISE(reader, arrow::ipc::RecordBatchStreamReader::Open(inputFile));

    auto tb = Runtime::TupleBuffer(10, schema);

    auto source = ArrowSource(format, reader, inputFile);

    source.fillBuffer(tb);

    return arrow::Status::OK();
}

int main(int argc, char* argv[]) {
    auto args = argparse::parse<FSSourceArgs>(argc, argv);

    arrow_main(args);

    return 0;
}
