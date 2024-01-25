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

    std::shared_ptr<Schema> append(SchemaFieldPtr&& field);

    size_t get_tuple_size() const;

    size_t get_field_offset(size_t index) const;

    size_t name_to_index(const std::string& field_name) {
        size_t i = 0;
        for (const auto& schema_field: fields) {
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

        uint8_t* data;
        DataType dt;
    };

    struct ConstTupleValueRef {
        template<typename T>
        [[nodiscard]] T read() const {
            return std::bit_cast<T>(data);
        }

        uint8_t const* data;
        DataType dt;
    };

    struct TupleRef {
        uint8_t* data;
        const std::vector<size_t>& offsets;

        TupleValueRef operator[](size_t index);

        ConstTupleValueRef operator[](size_t index) const;
    };

    struct ConstTupleRef {
        uint8_t const* data;
        const std::vector<size_t>& offsets;

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
            TupleIterator& operator++() {
                index++;
                return *this;
            }

            // Postfix increment
            TupleIterator operator++(int) {
                TupleIterator tmp = *this;
                ++(*this);
                return tmp;
            }

            friend bool operator==(const TupleIterator& a, const TupleIterator& b) { return a.index == b.index; };
            friend bool operator!=(const TupleIterator& a, const TupleIterator& b) { return a.index != b.index; };

            TupleIterator(Runtime::TupleBuffer& tb, size_t index)
                : tb(tb),
                  index(index) {
            }

        private:
            Runtime::TupleBuffer& tb;
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

        [[nodiscard]] const std::vector<uint8_t>& getBuffer() const {
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
    std::string getFormattedBuffer(Runtime::TupleBuffer& inputBuffer);

    /**
    * @brief method to get the schema from the arrow format
    * @return return the arrow schema
    */
    std::shared_ptr<arrow::Schema> getArrowSchema();

    void writeArrowArrayToTupleBuffer(uint64_t tupleCountInBuffer, uint64_t schemaFieldIndex,
                                      Runtime::TupleBuffer& tupleBuffer,
                                      std::shared_ptr<arrow::Array> arrowArray) const;

    /**
    * @brief method to get the arrow arrays from tuple buffer
    * @param a reference to input TupleBuffer
    * @return a vector of Arrow Arrays
    */
    std::vector<std::shared_ptr<arrow::Array>> getArrowArrays(Runtime::TupleBuffer& inputBuffer) const;

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
    std::vector<std::shared_ptr<arrow::Array>> buildArrowArrays();

    SchemaPtr schema;
};

#define NES_NOT_IMPLEMENTED() assert(false && "Not Implemented")


#endif //COMMON_HPP
