//
// Created by 王洪城 on 2024/5/1.
//

#include "leveldb/table_builder.h"
#include "table/filter_block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "util/crc32c.h"
#include "util/coding.h"
#include "leveldb/filter_policy.h"

namespace leveldb {
struct TableBuilder::Rep {
    Rep(const Options& opt, WritableFile* f)
        : options(opt), index_block_options(opt),
        file(f), offset(0), data_block(&options), index_block(&index_block_options),
        num_entries(0), closed(false),
        filter_block(opt.filter_policy == nullptr ? nullptr : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
        index_block_options.block_restart_interval = 1;
    }
    Options options;
    Options index_block_options;
    WritableFile* file;
    uint64_t offset;
    Status status;
    BlockBuilder data_block;
    BlockBuilder index_block;
    std::string last_key;
    int64_t num_entries;
    bool closed;
    FilterBlockBuilder* filter_block;

    bool pending_index_entry;
    BlockHandle pending_handle;
    std::string compressed_output;
};
TableBuilder::TableBuilder(const leveldb::Options &options, leveldb::WritableFile *file)
    : rep_(new Rep(options, file)){
    if (rep_->filter_block != nullptr) {
        rep_->filter_block->StartBlock(0);
    }
}

TableBuilder::~TableBuilder() {
    assert(rep_->closed);
    delete rep_->filter_block;
    delete rep_;
}

Status TableBuilder::ChangeOptions(const leveldb::Options &options) {
    if (options.comparator != rep_->options.comparator) {
        return Status::InvalidArgument("changing comparator while building table");
    }

    rep_->options = options;
    rep_->index_block_options = options;
    rep_->index_block_options.block_restart_interval = 1;
    return Status::OK();
}

void TableBuilder::Add(const leveldb::Slice &key, const leveldb::Slice &value) {
    Rep* r = rep_;
    assert(!r->closed);
    if (!ok()) return;
    if (r->num_entries > 0) {}
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);

    if (r->pending_index_entry) {
        assert(r->data_block.empty());
        r->options.comparator->FindShortestSeparator(&r->last_key, key);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding);
        r->index_block.Add(r->last_key, Slice(handle_encoding));
        r->pending_index_entry = false;
    }
    if (r->filter_block != nullptr) {
        r->filter_block->AddKey(key);
    }
    r->last_key.assign(key.data(), key.size());
    r->num_entries++;
    r->data_block.Add(key, value);

    const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
    if (estimated_block_size >= r->options.block_size) {
        Flush();
    }
}
void TableBuilder::Flush() {
    Rep* r = rep_;
    assert(!r->closed);

    if (!ok()) return;
    if (r->data_block.empty()) return;
    assert(!r->pending_index_entry);
    WriteBlock(&r->data_block, &r->pending_handle);
    if (ok()) {
        r->pending_index_entry = true;
        r->status = r->file->Flush();
    }
    if (r->filter_block != nullptr) {
        r->filter_block->StartBlock(r->offset);
    }

}
    void TableBuilder::Abandon() {
        Rep* r = rep_;
        assert(!r->closed);
        r->closed = true;
    }
    Status TableBuilder::status() const { return rep_->status; }

void TableBuilder::WriteRawBlock(const leveldb::Slice &block_contents, leveldb::CompressionType type,
                                 leveldb::BlockHandle *handle) {
    Rep* r = rep_;
    handle->set_offset(r->offset);
    handle->set_size(block_contents.size());
    r->status = r->file->Append(block_contents);
    if (r->status.ok()) {
        char trailer[kBlockTrailerSize];
        trailer[0] = type;
        uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
        crc = crc32c::Extend(crc, trailer, 1);
        EncodeFixed32(trailer+1, crc32c::Mask(crc));
        if (r->status.ok()) {}
        r->offset += block_contents.size() + kBlockTrailerSize;
    }
}
void TableBuilder::WriteBlock(leveldb::BlockBuilder *block, leveldb::BlockHandle *handle) {
    assert(ok());
    Rep* r = rep_;
    Slice raw = block->Finish();
    Slice block_contents;
    CompressionType type = r->options.compression;

    switch (type) {
        case kNoCompression:
            block_contents = raw;
            break;
        case kSnappyCompression: {
            std::string *compressed = &r->compressed_output;
            if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                block_contents = raw;
                type = kNoCompression;
            }
            break;
        }
        case kZstdCompression: {
            std::string *compressed = &r->compressed_output;
            if (port::Zstd_Compress(r->options.zstd_compression_level, raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                block_contents = raw;
                type = kNoCompression;
            }
            break;
        }
    }
    WriteRawBlock(block_contents, type, handle);
    r->compressed_output.clear();
    block->Reset();
}

Status TableBuilder::Finish() {
    Rep* r = rep_;
    Flush();
    assert(!r->closed);
    r->closed = true;

    BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

    if (ok() && r->filter_block != nullptr) {
        WriteRawBlock(r->filter_block->Finish(), kNoCompression, &filter_block_handle);
    }

    // wirte metaindex block
    if (ok()) {
        BlockBuilder meta_index_block(&r->options);
        if (r->filter_block != nullptr) {
            std::string key = "filter.";
            key.append(r->options.filter_policy->Name());
            std::string handle_encoding;
            filter_block_handle.EncodeTo(&handle_encoding);
            meta_index_block.Add(key, handle_encoding);
        }

        WriteBlock(&meta_index_block, &metaindex_block_handle);

        // write index block
        if (ok()) {
            if (r->pending_index_entry) {
                r->options.comparator->FindShortSuccessor(&r->last_key);
                std::string handle_encoding;
                r->pending_handle.EncodeTo(&handle_encoding);
                r->index_block.Add(r->last_key, Slice(handle_encoding));
                r->pending_index_entry = false;
            }
            WriteBlock(&r->index_block, &index_block_handle);
        }
         // write footer
         if (ok()) {
             Footer footer;
             footer.set_metaindex_handle(metaindex_block_handle);
             footer.set_index_handle(index_block_handle);
             std::string footer_encoding;
             footer.EncodeTo(&footer_encoding);
             r->status = r->file->Append(footer_encoding);
             if (r->status.ok()) {
                 r->offset += footer_encoding.size();
             }
         }
         return r->status;
    }
}

    uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

    uint64_t TableBuilder::FileSize() const { return rep_->offset; }

} // namespace leveldb