#include "RocksDBDictionary.h"


#include <algorithm>
#include <Common/typeid_cast.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"
#include <ext/map.h>
#include <ext/range.h>
#include <ext/size.h>
#include <ext/bit_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{
    constexpr size_t DEFAULT_SSD_BLOCK_SIZE = DEFAULT_AIO_FILE_BLOCK_SIZE;
    constexpr size_t DEFAULT_FILE_SIZE = 4 * 1024 * 1024 * 1024ULL;
    constexpr size_t DEFAULT_PARTITIONS_COUNT = 16;
    constexpr size_t DEFAULT_READ_BUFFER_SIZE = 16 * DEFAULT_SSD_BLOCK_SIZE;
    constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = DEFAULT_SSD_BLOCK_SIZE;
}

RocksDBDictionary::RocksDBDictionary(
        const std::string & name_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        const std::string & path_,
        const size_t max_partitions_count_,
        const size_t partition_size_,
        const size_t block_size_,
        const size_t read_buffer_size_,
        const size_t write_buffer_size_)
        : name(name_)
        , dict_struct(dict_struct_)
        , source_ptr(std::move(source_ptr_))
        , dict_lifetime(dict_lifetime_)
        , path(path_)
        , max_partitions_count(max_partitions_count_)
        , partition_size(partition_size_)
        , block_size(block_size_)
        , read_buffer_size(read_buffer_size_)
        , write_buffer_size(write_buffer_size_)
        , log(&Poco::Logger::get("RocksDBDictionary"))
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* tmp_db;
    rocksdb::Status status = rocksdb::DB::Open(options, "/mnt/disk4/clickhouse_dicts/rdb", &tmp_db);
    if (!status.ok())
        throw Exception{"Rocksdb open failed", ErrorCodes::UNSUPPORTED_METHOD};
    db.reset(tmp_db);
}

#define DECLARE(TYPE) \
void RocksDBDictionary::get##TYPE( \
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const \
{ \
    const auto index = getAttributeIndex(attribute_name); \
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
    const auto null_value = std::get<TYPE>(null_values[index]); \
    getItemsNumberImpl<TYPE, TYPE>( \
            index, \
            ids, \
            out, \
            [&](const size_t) { return null_value; }); \
}

DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
DECLARE(Decimal32)
DECLARE(Decimal64)
DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
void RocksDBDictionary::get##TYPE( \
    const std::string & attribute_name, \
    const PaddedPODArray<Key> & ids, \
    const PaddedPODArray<TYPE> & def, \
    ResultArrayType<TYPE> & out) const \
{ \
    const auto index = getAttributeIndex(attribute_name); \
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
    getItemsNumberImpl<TYPE, TYPE>( \
        index, \
        ids, \
        out, \
        [&](const size_t row) { return def[row]; }); \
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
DECLARE(Decimal32)
DECLARE(Decimal64)
DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
void RocksDBDictionary::get##TYPE( \
    const std::string & attribute_name, \
    const PaddedPODArray<Key> & ids, \
    const TYPE def, \
    ResultArrayType<TYPE> & out) const \
{ \
    const auto index = getAttributeIndex(attribute_name); \
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
    getItemsNumberImpl<TYPE, TYPE>( \
        index, \
        ids, \
        out, \
        [&](const size_t) { return def; }); \
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
DECLARE(Decimal32)
DECLARE(Decimal64)
DECLARE(Decimal128)
#undef DECLARE

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void RocksDBDictionary::getItemsNumberImpl(
        const size_t attribute_index, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const
{
    const auto now = std::chrono::system_clock::now();

    std::unordered_map<Key, std::vector<size_t>> not_found_ids;

    //storage.getValue<OutputType>(attribute_index, ids, out, not_found_ids, now);
    UNUSED(now);
    UNUSED(attribute_index);
    UNUSED(ids);
    UNUSED(out);
    UNUSED(get_default);

    std::vector<rocksdb::Slice> keys;
    keys.reserve(ids.size());
    for (const auto & id : ids)
        keys.emplace_back(&id, sizeof(id));

    if (not_found_ids.empty())
        return;

    std::vector<Key> required_ids(not_found_ids.size());
    std::transform(std::begin(not_found_ids), std::end(not_found_ids), std::begin(required_ids), [](const auto & pair) { return pair.first; });

    /*storage.update(
            source_ptr,
            required_ids,
            [&](const auto id, const auto row, const auto & new_attributes) {
                for (const size_t out_row : not_found_ids[id])
                    out[out_row] = std::get<PaddedPODArray<OutputType>>(new_attributes[attribute_index].values)[row];
            },
            [&](const size_t id)
            {
                for (const size_t row : not_found_ids[id])
                    out[row] = get_default(row);
            },
            getLifetime(),
            null_values);*/
}

void RocksDBDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    const auto null_value = StringRef{std::get<String>(null_values[index])};

    getItemsString(index, ids, out, [&](const size_t) { return null_value; });
}

void RocksDBDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsString(index, ids, out, [&](const size_t row) { return def->getDataAt(row); });
}

void RocksDBDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsString(index, ids, out, [&](const size_t) { return StringRef{def}; });
}

template <typename DefaultGetter>
void RocksDBDictionary::getItemsString(const size_t attribute_index, const PaddedPODArray<Key> & ids,
                                                      ColumnString * out, DefaultGetter && get_default) const
{
    UNUSED(attribute_index);
    UNUSED(ids);
    UNUSED(out);
    UNUSED(get_default);
}

void RocksDBDictionary::has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    const auto now = std::chrono::system_clock::now();
    UNUSED(now);
    UNUSED(ids);
    UNUSED(out);


    std::unordered_map<Key, std::vector<size_t>> not_found_ids;
    /*storage.has(ids, out, not_found_ids, now);*/
    if (not_found_ids.empty())
        return;

    std::vector<Key> required_ids(not_found_ids.size());
    std::transform(std::begin(not_found_ids), std::end(not_found_ids), std::begin(required_ids), [](const auto & pair) { return pair.first; });

    /*storage.update(
            source_ptr,
            required_ids,
            [&](const auto id, const auto, const auto &) {
                for (const size_t out_row : not_found_ids[id])
                    out[out_row] = true;
            },
            [&](const size_t id)
            {
                for (const size_t row : not_found_ids[id])
                    out[row] = false;
            },
            getLifetime(),
            null_values);*/
}

BlockInputStreamPtr RocksDBDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    UNUSED(column_names);
    UNUSED(max_block_size);
    //using BlockInputStreamType = DictionaryBlockInputStream<RocksDBDictionary, Key>;
    //ern std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, storage.getCachedIds(), column_names);
    return nullptr;
}

size_t RocksDBDictionary::getAttributeIndex(const std::string & attr_name) const
{
    auto it = attribute_index_by_name.find(attr_name);
    if (it == std::end(attribute_index_by_name))
        throw  Exception{"Attribute `" + name + "` does not exist.", ErrorCodes::BAD_ARGUMENTS};
    return it->second;
}

template <typename T>
AttributeValueVariant RocksDBDictionary::createAttributeNullValueWithTypeImpl(const Field & null_value)
{
    AttributeValueVariant var_null_value = static_cast<T>(null_value.get<NearestFieldType<T>>());
    if constexpr (std::is_same_v<String, T>)
    {
        bytes_allocated += sizeof(StringRef);
        //if (!string_arena)
        //    string_arena = std::make_unique<ArenaWithFreeLists>();
    }
    else
    {
        bytes_allocated += sizeof(T);
    }
    return var_null_value;
}

AttributeValueVariant RocksDBDictionary::createAttributeNullValueWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    switch (type)
    {
#define DISPATCH(TYPE) \
case AttributeUnderlyingType::ut##TYPE: \
    return createAttributeNullValueWithTypeImpl<TYPE>(null_value);

        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
        DISPATCH(String)
#undef DISPATCH
    }
    throw Exception{"Unknown attribute type: " + std::to_string(static_cast<int>(type)), ErrorCodes::TYPE_MISMATCH};
}

void RocksDBDictionary::createAttributes()
{
    null_values.reserve(dict_struct.attributes.size());
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
    {
        const auto & attribute = dict_struct.attributes[i];

        attribute_index_by_name.emplace(attribute.name, i);
        null_values.push_back(createAttributeNullValueWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

void registerDictionarySSDCache(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'cache'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{name
                            + ": elements .structure.range_min and .structure.range_max should be defined only "
                              "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const auto & layout_prefix = config_prefix + ".layout";

        const auto max_partitions_count = config.getInt(layout_prefix + ".ssd.max_partitions_count", DEFAULT_PARTITIONS_COUNT);
        if (max_partitions_count <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) max_partitions_count", ErrorCodes::BAD_ARGUMENTS};

        const auto block_size = config.getInt(layout_prefix + ".ssd.block_size", DEFAULT_SSD_BLOCK_SIZE);
        if (block_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto partition_size = config.getInt64(layout_prefix + ".ssd.partition_size", DEFAULT_FILE_SIZE);
        if (partition_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) partition_size", ErrorCodes::BAD_ARGUMENTS};
        if (partition_size % block_size != 0)
            throw Exception{name + ": partition_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto read_buffer_size = config.getInt64(layout_prefix + ".ssd.read_buffer_size", DEFAULT_READ_BUFFER_SIZE);
        if (read_buffer_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) read_buffer_size", ErrorCodes::BAD_ARGUMENTS};
        if (read_buffer_size % block_size != 0)
            throw Exception{name + ": read_buffer_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto write_buffer_size = config.getInt64(layout_prefix + ".ssd.write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE);
        if (write_buffer_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) write_buffer_size", ErrorCodes::BAD_ARGUMENTS};
        if (write_buffer_size % block_size != 0)
            throw Exception{name + ": write_buffer_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto path = config.getString(layout_prefix + ".ssd.path");
        if (path.empty())
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have empty path",
                            ErrorCodes::BAD_ARGUMENTS};

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<RocksDBDictionary>(
                name, dict_struct, std::move(source_ptr), dict_lifetime, path,
                max_partitions_count, partition_size / block_size, block_size,
                read_buffer_size / block_size, write_buffer_size / block_size);
    };
    factory.registerLayout("ssd", create_layout, false);
}

}
