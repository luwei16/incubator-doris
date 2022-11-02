#include "olap/rowset/segment_v2/inverted_index_writer.h"

#include <CLucene.h>
#include <CLucene/util/bkd/bkd_writer.h>

#include <memory>

#include "io/fs/local_file_system.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet_schema.h"

namespace doris::segment_v2 {
const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;

template <FieldType field_type>
class InvertedIndexColumnWriterImpl : public InvertedIndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

public:
    explicit InvertedIndexColumnWriterImpl(const std::string& field_name, uint32_t uuid,
                                           const std::string& segment_file_name,
                                           const std::string& dir, io::FileSystem* fs,
                                           const TabletIndex* index_meta)
            : _segment_file_name(segment_file_name),
              _directory(dir),
              _uuid(uuid),
              _fs(fs),
              _index_meta(index_meta) {
        _parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(_index_meta->properties()));
        _value_key_coder = get_key_coder(field_type);
        _field_name = std::wstring(field_name.begin(), field_name.end());
    };

    ~InvertedIndexColumnWriterImpl() override {
        try {
            if constexpr (field_is_slice_type(field_type)) {
                if (_index_writer) {
                    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(
                        _segment_file_name, _index_meta->index_id());
                    LOG(WARNING) << "inverted index column writer should be null here, close it "
                                 << index_file_name;
                }
                close();
            }
        } catch (...) {
            LOG(WARNING) << "inverted index column writer close error";
        }
    }

    Status init() override {
        if constexpr (field_is_slice_type(field_type)) {
            return init_fulltext_index();
        } else if constexpr (field_is_numeric_type(field_type)) {
            return init_bkd_index();
        }
        return Status::InternalError("field type not supported");
    }

    void close() {
        if (_index_writer) {
            _index_writer->close();
            // open index searcher into cache
            InvertedIndexSearcherCache::instance()->prune();
            auto index_file_name = InvertedIndexDescriptor::get_index_file_name(
                _segment_file_name, _index_meta->index_id());
            InvertedIndexSearcherCache::instance()->insert(_fs, _directory, index_file_name);
            _CLLDELETE(_index_writer);
            _index_writer = nullptr;
        }

        if (_doc) {
            _CLLDELETE(_doc);
            _doc = nullptr;
        }

        if (_default_analyzer) {
            _CLLDELETE(_default_analyzer);
            _default_analyzer = nullptr;
        }

        if (_standard_analyzer) {
            _CLLDELETE(_standard_analyzer);
            _standard_analyzer = nullptr;
        }
        if (_char_string_reader) {
            _CLDELETE(_char_string_reader);
            _char_string_reader = nullptr;
        }
    };

    Status init_bkd_index() {
        size_t value_length = sizeof(CppType);
        // NOTE: initialize with 0, set to max_row_id when finish.
        int32_t max_doc = 0;
        int32_t total_point_count = std::numeric_limits<std::int32_t>::max();
        _bkd_writer = std::make_shared<lucene::util::bkd::bkd_writer>(
                max_doc, DIMS, DIMS, value_length, MAX_LEAF_COUNT, MAXMBSortInHeap,
                total_point_count, true, config::max_depth_in_bkd_tree);
        return Status::OK();
    }

    Status init_fulltext_index() {
        bool create = true;

        auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                _directory + "/" + _segment_file_name, _index_meta->index_id());

        // LOG(INFO) << "inverted index path: " << index_path;

        if (lucene::index::IndexReader::indexExists(index_path.c_str())) {
            create = false;
            if (lucene::index::IndexReader::isLocked(index_path.c_str())) {
                LOG(INFO) << ("Lucene Index was locked... unlocking it.\n");
                lucene::index::IndexReader::unlock(index_path.c_str());
            }
        }

        _char_string_reader = _CLNEW lucene::util::SStringReader<char>;
        _doc = _CLNEW lucene::document::Document();
        _default_analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>();
        _standard_analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
#ifdef CLOUD_MODE
        _lfs = std::make_unique<doris::io::LocalFileSystem>(config::tmp_file_dir);
        auto lfs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
                config::tmp_file_dir + "/" + _segment_file_name, _uuid);
        lucene::store::Directory* dir = DorisCompoundDirectory::getDirectory(
                _lfs.get(), lfs_index_path.c_str(), true, _fs, index_path.c_str());
#else
        lucene::store::Directory* dir =
                DorisCompoundDirectory::getDirectory(_fs, index_path.c_str(), true);
#endif

        if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
            _index_writer =
                    _CLNEW lucene::index::IndexWriter(dir, _standard_analyzer, create, true);
        } else {
            // ANALYSER_NOT_SET, ANALYSER_SIMPLE use default SimpleAnalyzer
            _index_writer = _CLNEW lucene::index::IndexWriter(dir, _default_analyzer, create, true);
        }

        _index_writer->setMaxBufferedDocs(config::inverted_index_max_buffer_docs);
        _index_writer->setRAMBufferSizeMB(config::inverted_index_ram_buffer_size);
        _index_writer->setMaxFieldLength(MAX_FIELD_LEN);
        _index_writer->setMergeFactor(config::inverted_index_merge_factor);
        _index_writer->setUseCompoundFile(false);
        _doc->clear();

        int field_config =
                lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_NONORMS;
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
        } else {
            field_config |= lucene::document::Field::INDEX_TOKENIZED;
        }
        //auto name = lucene::util::Misc::_charToWide(fn.c_str());
        //field_name = std::wstring(fn.begin(), fn.end());

        _field = _CLNEW lucene::document::Field(_field_name.c_str(), field_config);
        _doc->add(*_field);
        // NOTE: need to ref_cnt-- for dir,
        // when index_writer is destroyed, if closeDir is set, dir will be close
        // _CLDECDELETE(dir) will try to ref_cnt--, when it decreases to 1, dir will be destroyed.
        _CLLDECDELETE(dir)

        return Status::OK();
    }

    Status add_nulls(uint32_t count) override {
        _rid += count;
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr) {
                LOG(ERROR) << "could not find field in fields map when add_nulls in inverted index "
                              "writer";
                return Status::InternalError("could not find field in clucene");
            }

            for (int i = 0; i < count; ++i) {
                std::string empty_value;
                auto empty_val = lucene::util::Misc::_charToWide(empty_value.c_str());
                _field->setValue(empty_val, false);
                _index_writer->addDocument(_doc);
            }
        }
        return Status::OK();
    }

    void new_fulltext_field(const char* field_value_data, size_t field_value_size) {
        auto field_value = lucene::util::Misc::_charToWide(field_value_data, field_value_size);
        int field_config = lucene::document::Field::STORE_NO;
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
        } else {
            field_config |= lucene::document::Field::INDEX_TOKENIZED;
        }
        bool duplicate_value = false;
        auto field = _CLNEW lucene::document::Field(_field_name.c_str(), field_value, field_config,
                                                    duplicate_value);
        _doc->add(*field);
        // Field did not duplicate value, so we don't have to delete field_value
        //_CLDELETE_ARRAY(field_value)
        //_CLDELETE(field);
    }

    Status add_values(const std::string fn, const void* values, size_t count) override {
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr) {
                LOG(ERROR) << "could not find field in fields map when add_values in inverted "
                              "index writer"
                           << fn;
                return Status::InternalError("could not find field in clucene");
            }
            auto* v = (Slice*)values;
            for (int i = 0; i < count; ++i) {
                if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
                    //auto field_value = lucene::util::Misc::_charToWide(v->get_data(), v->get_size());
                    char* value = v->mutable_data();
                    char* act_value = _CL_NEWARRAY(char, v->get_size() + 1);
                    for (auto i = 0; i < v->get_size(); i++) {
                        act_value[i] = value[i];
                    }
                    auto len = strlen(act_value);
                    _field->setValue(act_value, len);
                    ++v;
                    _rid++;
                    _index_writer->addDocument(_doc, _default_analyzer);
                    _CLDELETE_ARRAY(act_value);
                } else if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
                    auto field_value =
                            lucene::util::Misc::_charToWide(v->get_data(), v->get_size());
                    // NOTE: avoid another data copy for string reader init in documentWriterThreadState
                    auto stringReader = _CLNEW lucene::util::StringReader(
                            field_value, wcslen(field_value), false);
                    _field->setValue(stringReader);
                    ++v;
                    _rid++;
                    _index_writer->addDocument(_doc);
                    _CLDELETE_ARRAY(field_value)
                } else {
                    _char_string_reader->init(v->get_data(), v->get_size(), false);
                    auto stream = _default_analyzer->reusableTokenStream(_field_name.c_str(),
                                                                         _char_string_reader);
                    // NOTE: avoid another data copy for string reader init in documentWriterThreadState
                    /*auto stringReader = _CLNEW lucene::util::StringReader(
                            field_value, wcslen(field_value), false);*/
                    _field->setValue(stream);
                    ++v;
                    _rid++;
                    _index_writer->addDocument(_doc, _default_analyzer);
                }
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            add_numeric_values(values, count);
        }
        return Status::OK();
    }

    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override {
        auto* item_data_ptr = const_cast<CollectionValue*>(values)->mutable_data();

        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr) {
                LOG(ERROR)
                        << "could not find field in fields map when add_array_values in inverted "
                           "index writer";
                return Status::InternalError("could not find field in clucene");
            }
            _doc->clear();

            for (int i = 0; i < count; ++i) {
                for (size_t j = 0; j < values->length(); ++j) {
                    auto* v = (Slice*)item_data_ptr;

                    if (values->is_null_at(j)) {
                        std::string empty_value;
                        new_fulltext_field(empty_value.c_str(), empty_value.size());
                    } else {
                        new_fulltext_field(v->get_data(), v->get_size());
                    }
                    item_data_ptr = (uint8_t*)item_data_ptr + field_size;
                }
                _rid++;
                _index_writer->addDocument(_doc);
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            auto p = reinterpret_cast<const CppType*>(item_data_ptr);
            for (int i = 0; i < count; ++i) {
                for (size_t j = 0; j < values->length(); ++j) {
                    if (values->is_null_at(j)) {
                        // bkd do not index null values, so we do nothing here.
                    } else {
                        std::string new_value;
                        size_t value_length = sizeof(CppType);

                        _value_key_coder->full_encode_ascending(p, &new_value);
                        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);
                    }
                    p++;
                }
                _row_ids_seen_for_bkd++;
                _rid++;
            }
        }
        return Status::OK();
    }

    void add_numeric_values(const void* values, size_t count) {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value(*p);
            p++;
            _row_ids_seen_for_bkd++;
        }
    }

    void add_value(const CppType& value) {
        std::string new_value;
        size_t value_length = sizeof(CppType);

        _value_key_coder->full_encode_ascending(&value, &new_value);
        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);

        /*auto it = _mem_index.find(value);
        if (it != _mem_index.end()) {
            // exiting value, update bitmap
            it->second.add(_rid);
        } else {
            // new value, copy value and insert new key->bitmap pair
            //CppType new_value;
            //_typeinfo->deep_copy(&new_value, &value, &_pool);
            _mem_index.insert({value, roaring::Roaring::bitmapOf(1, _rid)});
            it = _mem_index.find(value);
        }*/
        _rid++;
    }

    uint64_t size() const override {
        //TODO: get size of inverted index
        return 0;
    }

    Status finish() override {
        if constexpr (field_is_numeric_type(field_type)) {
            auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                    _directory + "/" + _segment_file_name, _index_meta->index_id());
            if (_row_ids_seen_for_bkd == 0) {
                // if null data is flush, _row_ids_seen_for_bkd is empty.
                return Status::OK();
            }
#ifdef CLOUD_MODE
            if (_lfs == nullptr) {
                _lfs = std::make_unique<doris::io::LocalFileSystem>(config::tmp_file_dir);
            }
            auto lfs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
                    config::tmp_file_dir + "/" + _segment_file_name, _uuid);
            lucene::store::Directory* dir = DorisCompoundDirectory::getDirectory(
                    _lfs.get(), lfs_index_path.c_str(), true, _fs, index_path.c_str());
#else
            lucene::store::Directory* dir =
                    DorisCompoundDirectory::getDirectory(_fs, index_path.c_str(), true);
#endif
            _bkd_writer->max_doc_ = _rid;
            _bkd_writer->docs_seen_ = _row_ids_seen_for_bkd;
            std::unique_ptr<lucene::store::IndexOutput> out(dir->createOutput(
                    InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str()));
            _bkd_writer->meta_finish(out.get(), _bkd_writer->finish(out.get()), field_type);
            out->close();
            dir->close();
            _CLDELETE(dir)
        } else if constexpr (field_is_slice_type(field_type)) {
            close();
        }
        return Status::OK();
    }

private:
    //std::shared_ptr<MemTracker> _tracker;
    //MemPool _pool;
    //const TypeInfo* _typeinfo;
    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    roaring::Roaring _null_bitmap;
    uint64_t _reverted_index_size;

    lucene::document::Document* _doc;
    lucene::document::Field* _field;
    lucene::index::IndexWriter* _index_writer;
    lucene::util::SStringReader<char>* _char_string_reader;
    lucene::analysis::SimpleAnalyzer<char>* _default_analyzer;
    lucene::analysis::standard::StandardAnalyzer* _standard_analyzer;
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer;
    std::string _segment_file_name;
    std::string _directory;
    uint32_t _uuid;
    io::FileSystem* _fs;
#ifdef CLOUD_MODE
    std::unique_ptr<io::FileSystem> _lfs;
#endif
    const KeyCoder* _value_key_coder;
    const TabletIndex* _index_meta;
    InvertedIndexParserType _parser_type;
    std::wstring _field_name;
};

Status InvertedIndexColumnWriter::create(const Field* field,
                                         std::unique_ptr<InvertedIndexColumnWriter>* res,
                                         uint32_t uuid, const std::string& segment_file_name,
                                         const std::string& dir,
                                         const TabletIndex* index_meta,
                                         io::FileSystem* fs) {
    //RETURN_IF_ERROR(InvertedIndexDescriptor::init_index_directory(path));

    auto typeinfo = field->type_info();
    FieldType type = typeinfo->type();
    std::string field_name = field->name();
    if (type == OLAP_FIELD_TYPE_ARRAY) {
        const auto array_typeinfo = dynamic_cast<const ArrayTypeInfo*>(typeinfo);
        typeinfo = array_typeinfo->item_type_info();
        type = typeinfo->type();
    }

    switch (type) {
    case OLAP_FIELD_TYPE_CHAR: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_CHAR>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_VARCHAR: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_VARCHAR>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_STRING: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_STRING>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATETIME>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_DATE: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATE>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_TINYINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_TINYINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_SMALLINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_UNSIGNED_INT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_INT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_LARGEINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DECIMAL>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_BOOL: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_BOOL>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DOUBLE>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_FLOAT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_BIGINT>>(
                field_name, uuid, segment_file_name, dir, fs, index_meta);
        (*res)->init();
        break;
    }
    default:
        return Status::NotSupported("unsupported type for inverted index: " + std::to_string(type));
    }
    return Status::OK();
}
} // namespace doris::segment_v2
