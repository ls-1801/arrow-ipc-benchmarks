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

#ifndef COMMON_HPP
#define COMMON_HPP
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <brotli/encode.h>
#include <sstream>
#include <iostream>
#include <utility>
#include <cassert>
#include <memory>
#include <fmt/format.h>

#define NES_FATAL_ERROR(msg) assert(false && msg)
#define NES_ERROR(...) fmt::print(__VA_ARGS__)
#define NES_WARNING(...) fmt::print(__VA_ARGS__)
#define STR(x) #x
#define NES_ASSERT2_FMT(cond, ...) \
    do {if(!(cond)) fmt::print("Assertion Failed:\n" STR(cond) "\n{}", fmt::format(__VA_ARGS__));}while(0)
#define NES_TRACE(msg, ...)
#define NES_THROW_RUNTIME_ERROR(stream) do {\
    std::stringstream ss;\
ss << stream;\
throw std::runtime_error(ss.str());\
    }while(0)

enum DataType {
    INT_64,
    INT_32,
    INT_16,
    INT_8,
    UINT_64,
    UINT_32,
    UINT_16,
    UINT_8,
    FLOAT,
    DOUBLE,
    CHAR,
    BOOLEAN
};

class PhysicalType {
public:
    DataType nativeType;

    explicit PhysicalType(DataType native_type)
        : nativeType(native_type) {
    }

    bool isBasicType();

    size_t get_size() const;
};

using PhysicalTypePtr = std::shared_ptr<PhysicalType>;

class SchemaField {
    PhysicalTypePtr physical_type;
    std::string field_name;

    SchemaField(PhysicalTypePtr physical_type, std::string field_name)
        : physical_type(std::move(physical_type)),
          field_name(std::move(field_name)) {
    }

public:
    static std::shared_ptr<SchemaField> create(std::string field_name, DataType dt) {
        return std::shared_ptr<SchemaField>(new SchemaField(std::make_shared<PhysicalType>(dt), std::move(field_name)));
    }

    [[nodiscard]] std::string toString() const;

    PhysicalTypePtr getPhysicalType();

    [[nodiscard]] size_t get_size() const;
};

using SchemaFieldPtr = std::shared_ptr<SchemaField>;

class Schema : public std::enable_shared_from_this<Schema> {
    struct Private {
    };

public:
    Schema(Private) {
    }

    std::vector<SchemaFieldPtr> fields;
    std::vector<size_t> field_offset;

    static std::shared_ptr<Schema> create() {
        return std::make_shared<Schema>(Private{});
    }

    std::shared_ptr<Schema> append(SchemaFieldPtr &&field);

    size_t get_tuple_size() const;

    size_t get_field_offset(size_t index) const;

    size_t name_to_index(const std::string &field_name) {
        size_t i = 0;
        for (const auto &schema_field: fields) {
            if (schema_field->toString() == field_name) {
                return i;
            }
            i++;
        }

        std::cerr << "field name not found " << field_name << std::endl;
        assert(false && "FieldName not found");
    }
};

using SchemaPtr = std::shared_ptr<Schema>;

namespace Runtime {
    class BufferManager {
    };

    using BufferManagerPtr = std::shared_ptr<BufferManager>;

    struct TupleValueRef {
        template<typename T>
        [[nodiscard]] T read() const {
            T value;
            std::memcpy(&value, data, sizeof(T));
            return value;
        }

        template<typename T>
        void write(T value) {
            std::memcpy(data, &value, sizeof(T));
        }

        uint8_t *data;
        DataType dt;
    };

    struct ConstTupleValueRef {
        template<typename T>
        [[nodiscard]] T read() const {
            return std::bit_cast<T>(data);
        }

        uint8_t const *data;
        DataType dt;
    };

    struct TupleRef {
        uint8_t *data;
        const std::vector<size_t> &offsets;

        TupleValueRef operator[](size_t index);

        ConstTupleValueRef operator[](size_t index) const;
    };

    struct ConstTupleRef {
        uint8_t const *data;
        const std::vector<size_t> &offsets;

        ConstTupleValueRef operator[](size_t index) const;
    };

    class TupleBuffer {
    public:
        TupleBuffer(size_t capacity, SchemaPtr schema);

        size_t getNumberOfTuples();

        TupleRef operator[](size_t index);

        ConstTupleRef operator[](size_t index) const;

        [[nodiscard]] size_t getCapacity() const {
            return capacity;
        }

        SchemaPtr getSchema();

        struct TupleIterator {
            using iterator_category = std::forward_iterator_tag;
            using difference_type = std::ptrdiff_t;
            using reference = Runtime::TupleRef; // or also value_type&

            reference operator*() const { return tb[index]; }

            // Prefix increment
            TupleIterator &operator++() {
                index++;
                return *this;
            }

            // Postfix increment
            TupleIterator operator++(int) {
                TupleIterator tmp = *this;
                ++(*this);
                return tmp;
            }

            friend bool operator==(const TupleIterator &a, const TupleIterator &b) { return a.index == b.index; };
            friend bool operator!=(const TupleIterator &a, const TupleIterator &b) { return a.index != b.index; };

            TupleIterator(Runtime::TupleBuffer &tb, size_t index)
                : tb(tb),
                  index(index) {
            }

        private:
            Runtime::TupleBuffer &tb;
            size_t index;
        };

        TupleIterator begin() {
            return TupleIterator{*this, 0};
        }

        TupleIterator end() {
            return {*this, number_of_tuples};
        }

        void setNumberOfTuples(size_t number_of_tuples) {
            this->number_of_tuples = number_of_tuples;
        }

        [[nodiscard]] const std::vector<uint8_t> &getBuffer() const {
            return data;
        }

    private:
        std::vector<uint8_t> data;
        size_t number_of_tuples = 0;
        std::vector<size_t> field_offsets;
        size_t tuple_size;
        size_t capacity;
        SchemaPtr schema;
    };
}

class ArrowFormat {
public:
    ArrowFormat(SchemaPtr schema);

    virtual ~ArrowFormat() noexcept = default;

    /**
     * @brief Returns the schema of formatted according to the specific SinkFormat represented as string.
     * @return The formatted schema as string
     */
    std::string getFormattedSchema();

    /**
    * @brief method to format a TupleBuffer
    * @param a reference to input TupleBuffer
    * @return Formatted content of tuple buffer
     */
    std::string getFormattedBuffer(Runtime::TupleBuffer &inputBuffer);

    /**
    * @brief method to get the schema from the arrow format
    * @return return the arrow schema
    */
    std::shared_ptr<arrow::Schema> getArrowSchema();

    void writeArrowArrayToTupleBuffer(uint64_t tupleCountInBuffer, uint64_t schemaFieldIndex,
                                      Runtime::TupleBuffer &tupleBuffer,
                                      std::shared_ptr<arrow::Array> arrowArray) const;

    /**
    * @brief method to get the arrow arrays from tuple buffer
    * @param a reference to input TupleBuffer
    * @return a vector of Arrow Arrays
    */
    std::vector<std::shared_ptr<arrow::Array> > getArrowArrays(Runtime::TupleBuffer &inputBuffer) const;

    /**
     * @brief method to return the format as a string
     * @return format as string
     */
    std::string toString();

private:
    /**
    * @brief method that creates arrow arrays based on the schema
    * @return a vector of empty arrow arrays
    */
    std::vector<std::shared_ptr<arrow::Array> > buildArrowArrays();

    SchemaPtr schema;
};

#define NES_NOT_IMPLEMENTED() assert(false && "Not Implemented")

static std::shared_ptr<arrow::Schema> ICU_SCHEMA =
        std::make_shared<arrow::Schema>(arrow::FieldVector
                                        {
                                            std::make_shared<arrow::Field>("Albumin", arrow::uint64()),
                                            std::make_shared<arrow::Field>("ALP", arrow::uint32()),
                                            std::make_shared<arrow::Field>("ALT", arrow::uint8()),
                                            std::make_shared<arrow::Field>("AST", arrow::uint8()),
                                            std::make_shared<arrow::Field>("Bilirubin", arrow::uint8()),
                                            std::make_shared<arrow::Field>("BUN", arrow::uint32()),
                                            std::make_shared<arrow::Field>(
                                                "Cholesterol", arrow::float64(),
                                                true),
                                            std::make_shared<arrow::Field>(
                                                "Ceratinine", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "DiasABP", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "FiO2", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "GCS", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "Glucose", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "HCO3", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "HCT", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "HR", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "K", arrow::uint8()),
                                            std::make_shared<arrow::Field>(
                                                "Lactate", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "Mg", arrow::uint32()),
                                            std::make_shared<arrow::Field>(
                                                "MAP", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "MechVent", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "Na", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "NIDiasABP", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "NIMAP", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "NISysABP", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "PaCO2", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "PaO2", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "pH", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "Paletelts", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "RespRate", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "SaO2", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "SysABP", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "TEMP", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "Tropl", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "TropT", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "Urine", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "WBC", arrow::float64(), true),

                                        }, nullptr);


static std::shared_ptr<arrow::Schema> AQ_SCHEMA =
        std::make_shared<arrow::Schema>(arrow::FieldVector
                                        {
                                            std::make_shared<arrow::Field>("no", arrow::uint64()),
                                            std::make_shared<arrow::Field>("year", arrow::uint32()),
                                            std::make_shared<arrow::Field>("month", arrow::uint8()),
                                            std::make_shared<arrow::Field>("day", arrow::uint8()),
                                            std::make_shared<arrow::Field>("hour", arrow::uint8()),
                                            std::make_shared<arrow::Field>("PM2", arrow::uint32()),
                                            std::make_shared<arrow::Field>(
                                                "PM10", arrow::float64(),
                                                true),
                                            std::make_shared<arrow::Field>(
                                                "SO2", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "NO2", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "CO", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "O3", arrow::float64(), true),
                                            std::make_shared<arrow::Field>(
                                                "Temp", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "Pressure", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "Dewp", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "Rain", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "wd", arrow::uint8()),
                                            std::make_shared<arrow::Field>(
                                                "WSPM", arrow::float64()),
                                            std::make_shared<arrow::Field>(
                                                "station", arrow::uint32()),

                                        }, nullptr);


template<typename Builder>
std::shared_ptr<arrow::Array> iota(typename Builder::value_type start, size_t bufferSize) {
    Builder b;
    b.Reserve(bufferSize);
    for (size_t i = 0; i < bufferSize; i++) {
        b.Append(start + static_cast<std::remove_cvref_t<decltype(start)>>(i));
    }

    auto buildArray = b.Finish();
    if (!buildArray.ok()) {
        NES_FATAL_ERROR(
            "ArrowFormat::getArrowArrays: could not convert INT_8 field to arrow array.");
    }
    return *buildArray;
}

static inline std::shared_ptr<arrow::Schema> schema_by_name(std::string_view sv) {
    if (sv == "aq") {
        return AQ_SCHEMA;
    } else if (sv == "icu") {
        return ICU_SCHEMA;
    } else if (sv == "wifi") {
        std::vector<std::shared_ptr<arrow::Field> > fields;
        for (size_t i = 0; i < 671; i++) {
            fields.emplace_back(
                std::make_shared<arrow::Field>(fmt::format("signal_strength_{}", i), arrow::uint32(), true));
        }
        return std::make_shared<arrow::Schema>(fields);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

static inline std::shared_ptr<arrow::RecordBatch> generate_batch(std::shared_ptr<arrow::Schema> schema,
                                                                 size_t bufferSize, size_t counter) {
    std::vector<std::shared_ptr<arrow::Array> > arrowArrays;

    for (const auto &field: schema->fields()) {
        if (field->type() == arrow::float64()) {
            arrowArrays.emplace_back(iota<arrow::DoubleBuilder>(counter, bufferSize));
        } else if (field->type() == arrow::float32()) {
            arrowArrays.emplace_back(iota<arrow::FloatBuilder>(counter, bufferSize));
        } else if (field->type() == arrow::uint64()) {
            arrowArrays.emplace_back(iota<arrow::UInt64Builder>(counter, bufferSize));
        } else if (field->type() == arrow::uint8()) {
            arrowArrays.emplace_back(iota<arrow::UInt8Builder>(counter, bufferSize));
        } else if (field->type() == arrow::uint32()) {
            arrowArrays.emplace_back(iota<arrow::UInt32Builder>(counter, bufferSize));
        } else if (field->type() == arrow::int64()) {
            arrowArrays.emplace_back(iota<arrow::Int64Builder>(counter, bufferSize));
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    return arrow::RecordBatch::Make(std::move(schema), bufferSize, arrowArrays);
}

#endif //COMMON_HPP
