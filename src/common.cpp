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

#include "common.hpp"

std::string ArrowFormat::getFormattedSchema() {
    NES_NOT_IMPLEMENTED();
}

Runtime::TupleValueRef Runtime::TupleRef::operator[](size_t index) {
    return TupleValueRef{
        data + offsets[index], INT_64
    };
}

Runtime::ConstTupleValueRef Runtime::TupleRef::operator[](size_t index) const {
    return ConstTupleValueRef{
        data + offsets[index], INT_64
    };
}

Runtime::ConstTupleValueRef Runtime::ConstTupleRef::operator[](size_t index) const {
    return ConstTupleValueRef{
        data + offsets[index], INT_64
    };
}

Runtime::TupleBuffer::TupleBuffer(size_t capacity, SchemaPtr schema): capacity(capacity),
                                                                      tuple_size(schema->get_tuple_size()),
                                                                      field_offsets(schema->field_offset),
                                                                      schema(std::move(schema)) {
    data.resize(this->schema->get_tuple_size() * capacity);
}

size_t Runtime::TupleBuffer::getNumberOfTuples() {
    return number_of_tuples;
}

Runtime::TupleRef Runtime::TupleBuffer::operator[](size_t index) {
    return TupleRef{data.data() + index * tuple_size, field_offsets};
}

Runtime::ConstTupleRef Runtime::TupleBuffer::operator[](size_t index) const {
    assert(index < number_of_tuples);
    return ConstTupleRef{data.data() + index * tuple_size, field_offsets};
}

SchemaPtr Runtime::TupleBuffer::getSchema() {
    return schema;
}

bool PhysicalType::isBasicType() {
    return true;
}

size_t PhysicalType::get_size() const {
    switch (nativeType) {
        case INT_64:
            return sizeof(int64_t);
        case INT_32:
            return sizeof(int32_t);
        case INT_16:
            return sizeof(int16_t);
        case INT_8:
            return sizeof(int8_t);
        case UINT_64:
            return sizeof(uint64_t);
        case UINT_32:
            return sizeof(uint32_t);
        case UINT_16:
            return sizeof(uint16_t);
        case UINT_8:
            return sizeof(uint8_t);
        case FLOAT:
            return sizeof(float);
        case DOUBLE:
            return sizeof(double);
        case CHAR:
            return sizeof(char);
        case BOOLEAN:
            return sizeof(bool);
    }
    assert("Not implemented");
    return 0;
}

std::string SchemaField::toString() const {
    return field_name;
}

PhysicalTypePtr SchemaField::getPhysicalType() {
    return physical_type;
}

size_t SchemaField::get_size() const {
    return physical_type->get_size();
}

std::shared_ptr<Schema> Schema::append(SchemaFieldPtr &&field) {
    fields.push_back(field);
    if (field_offset.empty()) {
        field_offset.push_back(0);
    } else {
        field_offset.push_back(field_offset.back() + fields.back()->get_size());
    }

    return shared_from_this();
}

size_t Schema::get_tuple_size() const {
    return field_offset.back() + fields.back()->get_size();
}

size_t Schema::get_field_offset(size_t index) const {
    return field_offset[index];
}

ArrowFormat::ArrowFormat(SchemaPtr schema): schema(std::move(schema)) {
}

std::string ArrowFormat::getFormattedBuffer(Runtime::TupleBuffer &) {
    // since arrow writes it owns file separately along with the schema we do not need it
    NES_NOT_IMPLEMENTED();
}

std::string ArrowFormat::toString() { return "ARROW_IPC_FORMAT"; }


std::vector<std::shared_ptr<arrow::Array> > ArrowFormat::getArrowArrays(Runtime::TupleBuffer &inputBuffer) const {
    std::vector<std::shared_ptr<arrow::Array> > arrowArrays;
    uint64_t numberOfFields = schema->fields.size();
    auto numberOfTuples = inputBuffer.getNumberOfTuples();
    auto dynamicTupleBuffer = inputBuffer;

    // TODO #4082: add support for column layout

    // iterate over all fields in the schema to create respective arrow builders
    for (uint64_t columnIndex = 0; columnIndex < numberOfFields; ++columnIndex) {
        auto fieldName = schema->fields[columnIndex]->toString();
        auto physicalType = schema->fields[columnIndex]->getPhysicalType();
        try {
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = physicalType;
                switch (basicPhysicalType->nativeType) {
                    case DataType::INT_8: {
                        // create an int8 builder
                        arrow::Int8Builder int8Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int8_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<int8_t>();
                            auto append = int8Builder.Append(value);
                        }

                        // build the int8Array
                        auto buildArray = int8Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert INT_8 field to arrow array.");
                        }

                        // add the int8Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int8Array = *buildArray;
                        arrowArrays.push_back(int8Array);
                        break;
                    }
                    case DataType::INT_16: {
                        // create an int16 builder
                        arrow::Int16Builder int16Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int16_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<int16_t>();
                            auto append = int16Builder.Append(value);
                        }

                        // build the int16Array
                        auto buildArray = int16Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert INT_16 field to arrow array.");
                        }

                        // add the int16Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int16Array = *buildArray;
                        arrowArrays.push_back(int16Array);
                        break;
                    }
                    case DataType::INT_32: {
                        // create an int32 builder
                        arrow::Int32Builder int32Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int32_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<int32_t>();
                            auto append = int32Builder.Append(value);
                        }

                        // build the int32Array
                        auto buildArray = int32Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert INT_32 field to arrow array.");
                        }

                        // add the int32Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int32Array = *buildArray;
                        arrowArrays.push_back(int32Array);
                        break;
                    }
                    case DataType::INT_64: {
                        // create an int64 builder
                        arrow::Int64Builder int64Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int64_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<int64_t>();
                            auto append = int64Builder.Append(value);
                        }

                        // build the int64Array
                        auto buildArray = int64Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert INT_64 field to arrow array.");
                        }

                        // add the int64Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int64Array = *buildArray;
                        arrowArrays.push_back(int64Array);
                        break;
                    }
                    case DataType::UINT_8: {
                        // create an uint8 builder
                        arrow::UInt8Builder uint8Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint8_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<uint8_t>();
                            auto append = uint8Builder.Append(value);
                        }

                        // build the uint8Array
                        auto buildArray = uint8Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert UINT_8 field to arrow array.");
                        }

                        // add the uint8Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint8Array = *buildArray;
                        arrowArrays.push_back(uint8Array);
                        break;
                    }
                    case DataType::UINT_16: {
                        // create an uint16 builder
                        arrow::UInt16Builder uint16Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint16_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<uint16_t>();
                            auto append = uint16Builder.Append(value);
                        }

                        // build the uint16Array
                        auto buildArray = uint16Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert UINT_16 field to arrow array.");
                        }

                        // add the uint16Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint16Array = *buildArray;
                        arrowArrays.push_back(uint16Array);
                        break;
                    }
                    case DataType::UINT_32: {
                        // create an uint32 builder
                        arrow::UInt32Builder uint32Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint32_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<uint32_t>();
                            auto append = uint32Builder.Append(value);
                        }

                        // build the uint32Array
                        auto buildArray = uint32Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert UINT_32 field to arrow array.");
                        }

                        // add the uint32Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint32Array = *buildArray;
                        arrowArrays.push_back(uint32Array);
                        break;
                    }
                    case DataType::UINT_64: {
                        // create an uint64 builder
                        arrow::UInt64Builder uint64Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint64_t value = dynamicTupleBuffer[rowIndex][columnIndex].read<uint64_t>();
                            auto append = uint64Builder.Append(value);
                        }

                        // build the uint64Array
                        auto buildArray = uint64Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert UINT_64 field to arrow array.");
                        }

                        // add the uint64Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint64Array = *buildArray;
                        arrowArrays.push_back(uint64Array);
                        break;
                    }
                    case DataType::FLOAT: {
                        // create a float builder
                        arrow::FloatBuilder floatBuilder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            float value = dynamicTupleBuffer[rowIndex][columnIndex].read<float>();
                            auto append = floatBuilder.Append(value);
                        }

                        // build the floatArray
                        auto buildArray = floatBuilder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert FLOAT field to arrow array.");
                        }

                        // add the floatArray to the arrowArrays
                        std::shared_ptr<arrow::Array> floatArray = *buildArray;
                        arrowArrays.push_back(floatArray);
                        break;
                    }
                    case DataType::DOUBLE: {
                        // create a double builder
                        arrow::DoubleBuilder doubleBuilder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            double value = dynamicTupleBuffer[rowIndex][columnIndex].read<double>();
                            auto append = doubleBuilder.Append(value);
                        }

                        // build the doubleArray
                        auto buildArray = doubleBuilder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert DOUBLE field to arrow array.");
                        }

                        // add the doubleArray to the arrowArrays
                        std::shared_ptr<arrow::Array> doubleArray = *buildArray;
                        arrowArrays.push_back(doubleArray);
                        break;
                    }
                    case DataType::BOOLEAN: {
                        // create a boolean builder
                        arrow::BooleanBuilder booleanBuilder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            bool value = dynamicTupleBuffer[rowIndex][columnIndex].read<bool>();
                            auto append = booleanBuilder.Append(value);
                        }

                        // build the booleanArray
                        auto buildArray = booleanBuilder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR(
                                "ArrowFormat::getArrowArrays: could not convert BOOLEAN field to arrow array.");
                        }

                        // add the booleanArray to the arrowArrays
                        std::shared_ptr<arrow::Array> booleanArray = *buildArray;
                        arrowArrays.push_back(booleanArray);
                        break;
                    }
                    case DataType::CHAR: {
                        NES_FATAL_ERROR("ArrowFormat::getArrowArrays: type CHAR not supported by Arrow.");
                        throw std::invalid_argument("Arrow does not support CHAR");
                        break;
                    }
                }
            }
        } catch (const std::exception &e) {
            NES_ERROR("ArrowFormat::getArrowArrays: Failed to convert the arrowArray to desired NES data type. "
                      "Error: {}",
                      e.what());
        }
    }

    return arrowArrays;
}

std::shared_ptr<arrow::Schema> ArrowFormat::getArrowSchema() {
    std::vector<std::shared_ptr<arrow::Field> > arrowFields;
    std::shared_ptr<arrow::Schema> arrowSchema;
    uint64_t numberOfFields = schema->fields.size();

    // create arrow fields and add them to the field vector
    for (uint64_t i = 0; i < numberOfFields; i++) {
        auto fieldName = schema->fields[i]->toString();
        auto physicalType = schema->fields[i]->getPhysicalType();
        try {
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = physicalType;
                switch (basicPhysicalType->nativeType) {
                    case DataType::INT_8: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int8()));
                        break;
                    }
                    case DataType::INT_16: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int16()));
                        break;
                    }
                    case DataType::INT_32: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int32()));
                        break;
                    }
                    case DataType::INT_64: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int64()));
                        break;
                    }
                    case DataType::UINT_8: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint8()));
                        break;
                    }
                    case DataType::UINT_16: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint16()));
                        break;
                    }
                    case DataType::UINT_32: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint32()));
                        break;
                    }
                    case DataType::UINT_64: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint64()));
                        break;
                    }
                    case DataType::FLOAT: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::float32()));
                        break;
                    }
                    case DataType::DOUBLE: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::float64()));
                        break;
                    }
                    case DataType::BOOLEAN: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::boolean()));
                        break;
                    }
                    case DataType::CHAR: {
                        NES_FATAL_ERROR("ArrowFormat::getArrowSchema: type CHAR not supported by Arrow.");
                        throw std::invalid_argument("Arrow does not support CHAR");
                        break;
                    }
                }
            }
        } catch (const std::exception &e) {
            NES_ERROR("Failed to convert the arrowArray to desired NES data type. Error: {}", e.what());
        }
    }

    // build arrow schema
    arrowSchema = arrow::schema(arrowFields);
    return arrowSchema;
}

void ArrowFormat::writeArrowArrayToTupleBuffer(uint64_t tupleCountInBuffer,
                                               uint64_t schemaFieldIndex,
                                               Runtime::TupleBuffer &tupleBuffer,
                                               const std::shared_ptr<arrow::Array> arrowArray) const {
    if (arrowArray == nullptr) {
        NES_THROW_RUNTIME_ERROR("ArrowSource::writeArrowArrayToTupleBuffer: arrowArray is null.");
    }

    const auto &fields = schema->fields;
    auto dataType = fields[schemaFieldIndex];
    auto physicalType = dataType->getPhysicalType();
    uint64_t arrayLength = static_cast<uint64_t>(arrowArray->length());

    // nothing to be done if the array is empty
    if (arrayLength == 0) {
        return;
    }

    try {
        if (physicalType->isBasicType()) {
            auto basicPhysicalType = physicalType;

            switch (basicPhysicalType->nativeType) {
                case DataType::INT_8: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT8,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT8 data types. Type found"
                                    " in IPC file: {}",
                                    arrowArray->type()->ToString());

                    // cast the arrow array to the int8_t type
                    auto values = std::static_pointer_cast<arrow::Int8Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int8_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int8_t>(value);
                    }
                    break;
                }
                case DataType::INT_16: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT16,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT16 data types. Type found"
                                    " in IPC file: {}", arrowArray->type()->ToString());

                    // cast the arrow array to the int16_t type
                    auto values = std::static_pointer_cast<arrow::Int16Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int16_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int16_t>(value);
                    }
                    break;
                }
                case DataType::INT_32: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT32,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT32 data types. Type found"
                                    " in IPC file: {}", arrowArray->type()->ToString());

                    // cast the arrow array to the int8_t type
                    auto values = std::static_pointer_cast<arrow::Int32Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int32_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int32_t>(value);
                    }
                    break;
                }
                case DataType::INT_64: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT64,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT64 data types. Type found"
                                    " in IPC file:{}", arrowArray->type()->ToString());

                    // cast the arrow array to the int64_t type
                    auto values = std::static_pointer_cast<arrow::Int64Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int64_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int64_t>(value);
                    }
                    break;
                }
                case DataType::UINT_8: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT8,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT8 data types. Type found"
                                    " in IPC file:{}", arrowArray->type()->ToString());

                    // cast the arrow array to the uint8_t type
                    auto values = std::static_pointer_cast<arrow::UInt8Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint8_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint8_t>(value);
                    }
                    break;
                }
                case DataType::UINT_16: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT16,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT16 data types. Type found"
                                    " in IPC file:{}", arrowArray->type()->ToString());

                    // cast the arrow array to the uint16_t type
                    auto values = std::static_pointer_cast<arrow::UInt16Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint16_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint16_t>(value);
                    }
                    break;
                }
                case DataType::UINT_32: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT32,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT32 data types. Type found"
                                    " in IPC file:{}", arrowArray->type()->ToString());

                    // cast the arrow array to the uint32_t type
                    auto values = std::static_pointer_cast<arrow::UInt32Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint32_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint32_t>(value);
                    }
                    break;
                }
                case DataType::UINT_64: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT64,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT64 data types. Type found"
                                    " in IPC file:{}", arrowArray->type()->ToString());

                    // cast the arrow array to the uint64_t type
                    auto values = std::static_pointer_cast<arrow::UInt64Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint64_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint64_t>(value);
                    }
                    break;
                }
                case DataType::FLOAT: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::FLOAT,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent FLOAT data types. Type found"
                                    " in IPC file:{}", arrowArray->type()->ToString());

                    // cast the arrow array to the float type
                    auto values = std::static_pointer_cast<arrow::FloatArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        float value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<float>(value);
                    }
                    break;
                }
                case DataType::DOUBLE: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::DOUBLE,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent FLOAT64 data types. Type found"
                                    " in IPC file:{}", arrowArray->type()->ToString());

                    // cast the arrow array to the float64 type
                    auto values = std::static_pointer_cast<arrow::DoubleArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        double value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<double>(value);
                    }
                    break;
                }
                case DataType::CHAR: {
                    NES_FATAL_ERROR("ArrowSource::writeArrowArrayToTupleBuffer: type CHAR not supported by Arrow.");
                    throw std::invalid_argument("Arrow does not support CHAR");
                    break;
                }
                case DataType::BOOLEAN: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::BOOL,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent BOOL data types. Type found"
                                    " in file :{}", arrowArray->type()->ToString()
                                    + ", and type id: " + std::to_string(arrowArray->type()->id()));

                    // cast the arrow array to the boolean type
                    auto values = std::static_pointer_cast<arrow::BooleanArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        bool value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<bool>(value);
                    }
                    break;
                }
            }
        } else {
            // We do not support any other ARROW types (such as Lists, Maps, Tensors) yet. We could however later store
            // them in the childBuffers similar to how we store TEXT and push the computation supported by arrow down to
            // them.
            NES_NOT_IMPLEMENTED();
        }
    } catch (const std::exception &e) {
        NES_ERROR("Failed to convert the arrowArray to desired NES data type. Error: {}", e.what());
    }
}
