#include "olap/rowset/segment_v2/inverted_index_reader.h"

#include <CLucene/search/BooleanQuery.h>
#include <CLucene/search/PhraseQuery.h>
#include <CLucene/util/FutureArrays.h>
#include <CLucene/util/NumericUtils.h>

#include <filesystem>
#include <regex>

#include "common/config.h"
#include "gutil/strings/strip.h"
#include "io/fs/file_system.h"
#include "olap/key_coder.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/utils.h"
#include "runtime/string_value.h"
#include "util/time.h"

namespace doris {
namespace segment_v2 {

bool InvertedIndexReader::_is_match_query(InvertedIndexQueryType query_type) {
    return (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY);
}

std::vector<std::string> FullTextIndexReader::get_analyse_result(
        const std::wstring& field_name, const std::wstring& value,
        InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type) {
    std::vector<std::string> analyse_result;
    std::shared_ptr<lucene::analysis::Analyzer> analyzer;
    if (analyser_type == InvertedIndexParserType::PARSER_NOT_SET ||
        analyser_type == InvertedIndexParserType::PARSER_ENGLISH) {
        analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<TCHAR>>();
    } else if (analyser_type == InvertedIndexParserType::PARSER_STANDARD) {
        analyzer = std::make_shared<lucene::analysis::standard::StandardAnalyzer>();
    } else {
        // default
        analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<TCHAR>>();
    }

    std::unique_ptr<lucene::util::StringReader> reader(
            new lucene::util::StringReader(value.c_str()));
    std::unique_ptr<lucene::analysis::TokenStream> token_stream(
            analyzer->tokenStream(field_name.c_str(), reader.get()));

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        std::string tk = lucene::util::Misc::toString(
            token.termBuffer<TCHAR>(), token.termLength<TCHAR>());
        analyse_result.emplace_back(tk);
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    if (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
        query_type == InvertedIndexQueryType::MATCH_ALL_QUERY) {
        std::set<std::string> unrepeated_result(analyse_result.begin(), analyse_result.end());
        analyse_result.assign(unrepeated_result.begin(), unrepeated_result.end());
    }

    return analyse_result;
}

bool InvertedIndexReader::indexExists(io::Path& index_file_path) {
    bool exists = false;
    RETURN_IF_ERROR(_fs->exists(index_file_path, &exists));
    return exists;
}

Status FullTextIndexReader::new_iterator(InvertedIndexParserType analyser_type,
                                         InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(analyser_type, this);
    return Status::OK();
}

Status FullTextIndexReader::query(const std::string& column_name, const void* query_value,
                                  InvertedIndexQueryType query_type,
                                  InvertedIndexParserType analyser_type,
                                  roaring::Roaring* bit_map) {
    std::string search_str = reinterpret_cast<const StringValue*>(query_value)->to_string();
    LOG(INFO) << column_name
              << " begin to load the fulltext index from clucene, query_str=" << search_str;
    std::unique_ptr<lucene::search::Query> query;
    std::wstring field_ws = std::wstring(column_name.begin(), column_name.end());
    std::wstring search_str_ws = std::wstring(search_str.begin(), search_str.end());
    try {
        std::vector<std::string> analyse_result =
                get_analyse_result(field_ws, search_str_ws, query_type, analyser_type);

        if (analyse_result.empty()) {
            return Status::OLAPInternalError(
                    OLAP_ERR_INVERTED_INDEX_INVALID_PARAMETERS,
                    fmt::format("invalid input query_str '{}', please check your query sql",
                                search_str));
        }

        switch (query_type) {
        case InvertedIndexQueryType::MATCH_ANY_QUERY: {
            query.reset(_CLNEW lucene::search::BooleanQuery());
            for (auto token : analyse_result) {
                std::wstring token_ws = std::wstring(token.begin(), token.end());
                lucene::index::Term* term =
                        _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
                static_cast<lucene::search::BooleanQuery*>(query.get())
                        ->add(_CLNEW lucene::search::TermQuery(term), true,
                              lucene::search::BooleanClause::SHOULD);
                _CLDECDELETE(term);
            }
            break;
        }
        case InvertedIndexQueryType::MATCH_ALL_QUERY: {
            query.reset(_CLNEW lucene::search::BooleanQuery());
            for (auto token : analyse_result) {
                std::wstring token_ws = std::wstring(token.begin(), token.end());
                lucene::index::Term* term =
                        _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
                static_cast<lucene::search::BooleanQuery*>(query.get())
                        ->add(_CLNEW lucene::search::TermQuery(term), true,
                              lucene::search::BooleanClause::MUST);
                _CLDECDELETE(term);
            }
            break;
        }
        case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
            return Status::OLAPInternalError(
                    OLAP_ERR_INVERTED_INDEX_NOT_SUPPORTED,
                    fmt::format("match phrase of fulltext is not supported"));
            // query.reset(_CLNEW lucene::search::PhraseQuery());
            // for(auto token : analyse_result) {
            //     std::wstring token_ws = std::wstring(token.begin(), token.end());
            //     lucene::index::Term* term = _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
            //     static_cast<lucene::search::PhraseQuery*>(query.get())->add(term);
            //     _CLDECDELETE(term);
            // }
            break;
        }
        default:
            LOG(ERROR) << "fulltext query do not support query type other than match.";
            return Status::OLAPInternalError(
                    OLAP_ERR_INVERTED_INDEX_NOT_SUPPORTED,
                    fmt::format(
                            "fulltext query do not support query type other than match, column: {}",
                            column_name));
        }

    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured: " << e.what();
        return Status::OLAPInternalError(
                OLAP_ERR_INVERTED_INDEX_CLUCENE_ERROR,
                fmt::format("CLuceneError occured, error msg: {}", e.what()));
    }

    io::Path path(_path);
    auto index_dir = path.parent_path();
    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(path.filename(), _uuid);

    // check index file existence
    auto index_file_path = index_dir / index_file_name;
    if (!indexExists(index_file_path)) {
        LOG(WARNING) << "inverted index path: " << index_file_path.string() << " not exist.";
        return Status::OLAPInternalError(
                OLAP_ERR_INVERTED_INDEX_FILE_NOT_FOUND,
                fmt::format("inverted index path {} not found", index_file_path.string()));
    }

    roaring::Roaring result;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    InvertedIndexSearcherCache::instance()->get_index_searcher(
            _fs, index_dir.c_str(), index_file_name, &inverted_index_cache_handle);
    auto index_searcher = inverted_index_cache_handle.get_index_searcher();

    try {
        index_searcher->_search(query.get(),
                                [&result](const int32_t docid, const float_t /*score*/) {
                                    // docid equal to rowid in segment
                                    result.add(docid);
                                });
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured: " << e.what();
        return Status::OLAPInternalError(
                OLAP_ERR_INVERTED_INDEX_CLUCENE_ERROR,
                fmt::format("CLuceneError occured, error msg: {}", e.what()));
    }
    bit_map->swap(result);
    return Status::OK();
}

InvertedIndexReaderType FullTextIndexReader::type() {
    return InvertedIndexReaderType::FULLTEXT;
}

Status StringTypeInvertedIndexReader::new_iterator(InvertedIndexParserType analyser_type,
                                                   InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(analyser_type, this);
    return Status::OK();
}

Status StringTypeInvertedIndexReader::query(const std::string& column_name, const void* query_value,
                                            InvertedIndexQueryType query_type,
                                            InvertedIndexParserType analyser_type,
                                            roaring::Roaring* bit_map) {
    std::string search_str = reinterpret_cast<const StringValue*>(query_value)->to_string();
    LOG(INFO) << "begin to query the inverted index from clucene"
              << ", column_name: " << column_name << ", search_str: " << search_str;
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());
    std::wstring search_str_ws = std::wstring(search_str.begin(), search_str.end());
    lucene::index::Term* term =
            new lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str());
    std::unique_ptr<lucene::search::Query> query;

    io::Path path(_path);
    auto index_dir = path.parent_path();
    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(path.filename(), _uuid);

    // check index file existence
    auto index_file_path = index_dir / index_file_name;
    if (!indexExists(index_file_path)) {
        LOG(WARNING) << "inverted index path: " << index_file_path.string() << " not exist.";
        return Status::OLAPInternalError(
                OLAP_ERR_INVERTED_INDEX_FILE_NOT_FOUND,
                fmt::format("inverted index path {} not found", index_file_path.string()));
    }

    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        query.reset(new lucene::search::TermQuery(term));
        break;
    }
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        query.reset(new lucene::search::RangeQuery(nullptr, term, false));
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        query.reset(new lucene::search::RangeQuery(nullptr, term, true));
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        query.reset(new lucene::search::RangeQuery(term, nullptr, false));
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        query.reset(new lucene::search::RangeQuery(term, nullptr, true));
        break;
    }
    default:
        LOG(ERROR) << "invalid query type when query untokenized inverted index";
        if (_is_match_query(query_type)) {
            return Status::OLAPInternalError(
                    OLAP_ERR_INVERTED_INDEX_NOT_SUPPORTED,
                    fmt::format("'{}' is untokenized inverted index, please use equal query "
                                "instead of match query",
                                column_name));
        }
        return Status::OLAPInternalError(
                OLAP_ERR_INVERTED_INDEX_NOT_SUPPORTED,
                fmt::format("invalid query type when query untokenized inverted index"));
    }

    roaring::Roaring result;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    InvertedIndexSearcherCache::instance()->get_index_searcher(
            _fs, index_dir.c_str(), index_file_name, &inverted_index_cache_handle);
    auto index_searcher = inverted_index_cache_handle.get_index_searcher();

    try {
        index_searcher->_search(query.get(),
                                [&result](const int32_t docid, const float_t /*score*/) {
                                    // docid equal to rowid in segment
                                    result.add(docid);
                                });
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured: " << e.what();
        return Status::OLAPInternalError(
                OLAP_ERR_INVERTED_INDEX_CLUCENE_ERROR,
                fmt::format("CLuceneError occured, error msg: {}", e.what()));
    }

    bit_map->swap(result);
    return Status::OK();
}

InvertedIndexReaderType StringTypeInvertedIndexReader::type() {
    return InvertedIndexReaderType::STRING_TYPE;
}

BkdIndexReader::BkdIndexReader(io::FileSystem* fs, const std::string& path, const uint32_t uniq_id)
        : InvertedIndexReader(fs, path, uniq_id), compoundReader(nullptr) {
    io::Path io_path(_path);
    auto index_dir = io_path.parent_path();
    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(io_path.filename(), _uuid);

    // check index file existence
    auto index_file = index_dir / index_file_name;
    if (!indexExists(index_file)) {
        LOG(WARNING) << "bkd index: " << index_file.string() << " not exist.";
        return;
    }
    compoundReader = new DorisCompoundReader(
            DorisCompoundDirectory::getDirectory(fs, index_dir.c_str()), index_file_name.c_str());
}

Status BkdIndexReader::new_iterator(InvertedIndexParserType analyser_type,
                                    InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(analyser_type, this);
    return Status::OK();
}

Status BkdIndexReader::bkd_query(const std::string& column_name, const void* query_value,
                                 InvertedIndexQueryType query_type, InvertedIndexVisitor* visitor) {
    std::shared_ptr<lucene::util::bkd::bkd_reader> r;
    lucene::util::bkd::bkd_reader* tmp_reader;
    auto status = get_bkd_reader(tmp_reader);
    if (!status.ok()) {
        LOG(WARNING) << "get bkd reader for column " << column_name
                     << " failed: " << status.get_error_msg();
        return status;
    }
    r.reset(tmp_reader);
    std::string min; //("",r->bytes_per_dim_);
    std::string max; //("",r->bytes_per_dim_);
    char tmp[r->bytes_per_dim_];
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        _value_key_coder->full_encode_ascending(query_value, &max);
        _value_key_coder->full_encode_ascending(query_value, &min);
        break;
    }
    case InvertedIndexQueryType::LESS_THAN_QUERY:
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        _value_key_coder->full_encode_ascending(query_value, &max);
        _type_info->set_to_min(tmp);
        _value_key_coder->full_encode_ascending(tmp, &min);
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        _value_key_coder->full_encode_ascending(query_value, &min);
        _type_info->set_to_max(tmp);
        _value_key_coder->full_encode_ascending(tmp, &max);
        break;
    }
    default:
        LOG(ERROR) << "invalid query type when query bkd index";
        return Status::OLAPInternalError(OLAP_ERR_INVERTED_INDEX_NOT_SUPPORTED,
                                         fmt::format("invalid query type when query bkd index"));
    }
    visitor->set_min((uint8_t*)min.data());
    visitor->set_max((uint8_t*)max.data());
    visitor->set_reader(r.get());
    r->intersect(visitor);
    return Status::OK();
}

Status BkdIndexReader::try_query(const std::string& column_name, const void* query_value,
                                 InvertedIndexQueryType query_type,
                                 InvertedIndexParserType analyser_type, uint32_t* count) {
    uint64_t start = UnixMillis();
    auto v = std::make_unique<InvertedIndexVisitor>(nullptr, nullptr, nullptr, query_type, true);
    bkd_query(column_name, query_value, query_type, v.get());
    *count = v->get_num_hits();
    LOG(INFO) << "BKD index try search time taken: " << UnixMillis() - start << "ms "
              << " column: " << column_name << " result: " << v->get_num_hits();
    return Status::OK();
}

Status BkdIndexReader::query(const std::string& column_name, const void* query_value,
                             InvertedIndexQueryType query_type,
                             InvertedIndexParserType analyser_type, roaring::Roaring* bit_map) {
    uint64_t start = UnixMillis();
    auto v = std::make_unique<InvertedIndexVisitor>(nullptr, nullptr, bit_map, query_type);
    bkd_query(column_name, query_value, query_type, v.get());
    LOG(INFO) << "BKD index search time taken: " << UnixMillis() - start << "ms "
              << " column: " << column_name << " result: " << bit_map->cardinality();
    return Status::OK();
}

Status BkdIndexReader::get_bkd_reader(lucene::util::bkd::bkd_reader*& bkdReader) {
    // bkd file reader
    if (compoundReader == nullptr) {
        return Status::OLAPInternalError(OLAP_ERR_INVERTED_INDEX_FILE_NOT_FOUND,
                                         fmt::format("bkd index input file not found"));
    }
    CLuceneError err;
    lucene::store::IndexInput* in;
    if (!compoundReader->openInput(
                InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str(), in, err)) {
        return Status::OLAPInternalError(OLAP_ERR_INVERTED_INDEX_FILE_NOT_FOUND,
                                         fmt::format("bkd index input error: {}", err.what()));
    }

    bkdReader = new lucene::util::bkd::bkd_reader(in);
    bkdReader->read_type();
    bkdReader->read_index();

    _type_info = get_scalar_type_info((FieldType)bkdReader->type);
    if (_type_info == nullptr) {
        auto type = bkdReader->type;
        delete bkdReader;
        return Status::OLAPInternalError(
                OLAP_ERR_INVERTED_INDEX_NOT_SUPPORTED,
                fmt::format("unsupported typeinfo, type={}", type));
    }
    _value_key_coder = get_key_coder(_type_info->type());
    return Status::OK();
}

InvertedIndexReaderType BkdIndexReader::type() {
    return InvertedIndexReaderType::BKD;
}

InvertedIndexVisitor::InvertedIndexVisitor(const uint8_t* qMin, const uint8_t* qMax,
                                           roaring::Roaring* h, InvertedIndexQueryType query_type,
                                           bool only_count)
        : queryMin(qMin),
          queryMax(qMax),
          hits(h),
          num_hits(0),
          only_count(only_count),
          query_type(query_type) {}

bool InvertedIndexVisitor::matches(uint8_t* packedValue) {
    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;
        if (query_type == InvertedIndexQueryType::LESS_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) >= 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        } else if (query_type == InvertedIndexQueryType::GREATER_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) <= 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        } else {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) < 0) {
                // Doc's value is too low, in this dimension
                return false;
            }
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) > 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        }
    }
    return true;
}

void InvertedIndexVisitor::visit(int rowID) {
    if (only_count) {
        num_hits++;
    } else {
        hits->add(rowID);
    }
    if (0) {
        std::wcout << L"visit docID=" << rowID << std::endl;
    }
}

void InvertedIndexVisitor::visit(int rowID, std::vector<uint8_t>& packedValue) {
    if (0) {
        int x = lucene::util::NumericUtils::sortableBytesToLong(packedValue, 0);
        std::wcout << L"visit docID=" << rowID << L" x=" << x << std::endl;
    }
    if (matches(packedValue.data())) {
        if (only_count) {
            num_hits++;
        } else {
            hits->add(rowID);
        }
    }
}

lucene::util::bkd::relation InvertedIndexVisitor::compare(std::vector<uint8_t>& minPacked,
                                                          std::vector<uint8_t>& maxPacked) {
    bool crosses = false;

    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;

        if (query_type == InvertedIndexQueryType::LESS_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        minPacked.data(), offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) >= 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        } else if (query_type == InvertedIndexQueryType::GREATER_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        maxPacked.data(), offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) <= 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        } else {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        minPacked.data(), offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) > 0 ||
                lucene::util::FutureArrays::CompareUnsigned(
                        maxPacked.data(), offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) < 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        }

        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           minPacked.data(), offset, offset + reader->bytes_per_dim_, queryMin,
                           offset, offset + reader->bytes_per_dim_) <= 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           maxPacked.data(), offset, offset + reader->bytes_per_dim_, queryMax,
                           offset, offset + reader->bytes_per_dim_) >= 0;
    }

    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

Status InvertedIndexIterator::read_from_inverted_index(const std::string& column_name,
                                                       const void* query_value,
                                                       InvertedIndexQueryType query_type,
                                                       roaring::Roaring* bit_map) {
    if (_reader->type() == InvertedIndexReaderType::BKD) {
        auto query_bkd_limit_percent = config::query_bkd_inverted_index_limit_percent;
        uint32_t hit_count = 0;
        RETURN_IF_ERROR(try_read_from_inverted_index(column_name, query_value, query_type, &hit_count));
        if (hit_count > _segment_num_rows * query_bkd_limit_percent / 100) {
            LOG(INFO) << "hit count: " << hit_count 
                    << ", reached limit " << query_bkd_limit_percent << "%, segment num rows: " << _segment_num_rows;
            return Status::OLAPInternalError(OLAP_ERR_INVERTED_INDEX_HIT_LIMIT,
                    fmt::format("hit count '{}' for bkd inverted reached limit '{}%', segment num rows: {}",
                                hit_count, query_bkd_limit_percent, _segment_num_rows));
        }
    }
   
    RETURN_IF_ERROR(_reader->query(column_name, query_value, query_type, _analyser_type, bit_map));
    return Status::OK();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const std::string& column_name,
                                                           const void* query_value,
                                                           InvertedIndexQueryType query_type,
                                                           uint32_t* count) {
    // NOTE: only bkd index support try read now.
    if (query_type == InvertedIndexQueryType::GREATER_EQUAL_QUERY ||
        query_type == InvertedIndexQueryType::GREATER_THAN_QUERY ||
        query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY ||
        query_type == InvertedIndexQueryType::LESS_THAN_QUERY ||
        query_type == InvertedIndexQueryType::EQUAL_QUERY) {
        RETURN_IF_ERROR(
                _reader->try_query(column_name, query_value, query_type, _analyser_type, count));
    }
    return Status::OK();
}

InvertedIndexParserType InvertedIndexIterator::get_inverted_index_analyser_type() const {
    return _analyser_type;
}

InvertedIndexReaderType InvertedIndexIterator::get_inverted_index_reader_type() const {
    return _reader->type();
}

void InvertedIndexIterator::set_segment_num_rows(uint32_t num) {
    _segment_num_rows = num;
}

} // namespace segment_v2
} // namespace doris
