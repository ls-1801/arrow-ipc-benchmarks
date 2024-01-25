//
// Created by ls on 12.01.24.
//

#ifndef MEMORYIPC_HPP
#define MEMORYIPC_HPP

#include <iomanip>
#include <optional>
#include <queue>
#include <ranges>
#include <span>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <arrow/api.h>

#include "common.hpp"


struct Descriptor {
    boost::interprocess::managed_shared_memory::handle_t handle;
    size_t size;
};

struct DescriptorView {
    Descriptor internal;
    boost::interprocess::managed_shared_memory* segment;

    [[nodiscard]] std::span<uint8_t> get_memory() const {
        void* ptr = segment->get_address_from_handle(internal.handle);
        assert(reinterpret_cast<uintptr_t>(ptr) % 64 == 0);
        return {static_cast<uint8_t *>(ptr), internal.size};
    }

    [[nodiscard]] std::shared_ptr<arrow::Buffer> as_mutable_buffer() const {
        auto memory = get_memory();
        return arrow::MutableBuffer::Wrap(memory.data(), memory.size());
    }

    [[nodiscard]] std::shared_ptr<arrow::Buffer> as_buffer() const {
        auto memory = get_memory();
        return arrow::Buffer::Wrap(memory.data(), memory.size());
    }
};

struct QueueControlBlock {
    size_t read_idx = 0;
    size_t write_idx = 0;
    size_t size = 0;
    Descriptor* queue = nullptr;
    size_t capacity = 0;
};

struct source {
};

struct sink {
};

template<typename source_or_sink>
struct Queue {
    ~Queue() {
        if constexpr (std::is_same_v<source, source_or_sink>) {
            boost::interprocess::named_mutex::remove(fmt::format("{}_cb_mtx", shared_memory_name).c_str());
            boost::interprocess::shared_memory_object::remove(shared_memory_name.c_str());
        }
    }

    Queue(std::string name, size_t queue_length = 1024, size_t memory_size = 1024 * 1024 * 8)
        : shared_memory_name(std::move(name)) {
        if constexpr (std::is_same_v<source, source_or_sink>) {
            boost::interprocess::shared_memory_object::remove(shared_memory_name.c_str());
            segment = boost::interprocess::managed_shared_memory(boost::interprocess::create_only,
                                                                 shared_memory_name.c_str(),
                                                                 memory_size);
            mtx.emplace(boost::interprocess::create_only, fmt::format("{}_cb_mtx", shared_memory_name).c_str());
            auto cb_queue = segment.construct<Descriptor>(fmt::format("{}_cb_queue", shared_memory_name).c_str())[
                queue_length]();
            assert(cb_queue != nullptr);
            cb = segment.construct<QueueControlBlock>(fmt::format("{}_cb", shared_memory_name).c_str())();
            assert(cb != nullptr);
            cb->capacity = queue_length;
            cb->queue = cb_queue;
        } else {
            static_assert(std::is_same_v<sink, source_or_sink>, "Either source or sink");
            segment = boost::interprocess::managed_shared_memory(boost::interprocess::open_only,
                                                                 shared_memory_name.c_str());
            mtx.emplace(boost::interprocess::open_only, fmt::format("{}_cb_mtx", shared_memory_name).c_str());
            std::tie(cb, std::ignore) = segment.find<QueueControlBlock>(
                fmt::format("{}_cb", shared_memory_name).c_str());
            assert(cb != nullptr);
            assert(cb->queue != nullptr);
            assert(cb->capacity > 0);
        }
    }

    QueueControlBlock* cb;
    std::string shared_memory_name;
    std::optional<boost::interprocess::named_mutex> mtx;
    boost::interprocess::managed_shared_memory segment;

    std::optional<DescriptorView> read_buffer() {
        boost::interprocess::scoped_lock<boost::interprocess::named_mutex> lock(*mtx);
        if (cb->size > 0) {
            --cb->size;
            auto buffer = cb->queue[cb->read_idx];
            cb->read_idx = (cb->read_idx + 1) % cb->capacity;
            return DescriptorView{{buffer.handle, buffer.size}, &segment};
        }
        return std::nullopt;
    }

    DescriptorView allocate_buffer(size_t size) {
        void* ptr = segment.allocate_aligned(size, 64);
        auto handle = segment.get_handle_from_address(ptr);
        return {Descriptor{handle, size}, &segment};
    }

    void free_buffer(DescriptorView&& descriptor) {
        segment.deallocate(descriptor.get_memory().data());
    }

    std::optional<DescriptorView> write_buffer(DescriptorView&& descriptor) {
        boost::interprocess::scoped_lock<boost::interprocess::named_mutex> lock(*mtx);
        if (cb->size < cb->capacity - 1) {
            ++cb->size;
            cb->queue[cb->write_idx] = descriptor.internal;
            cb->write_idx = (cb->write_idx + 1) % cb->capacity;
            return std::nullopt;
        }
        return descriptor;
    }
};

using SourceQueue = Queue<source>;
using SinkQueue = Queue<sink>;

#endif //MEMORYIPC_HPP
