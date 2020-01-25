#pragma once

#include <variant>
#include <common/logger_useful.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include <rocksdb/db.h>

namespace DB
{

using AttributeValueVariant = std::variant<
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        UInt128,
        Int8,
        Int16,
        Int32,
        Int64,
        Decimal32,
        Decimal64,
        Decimal128,
        Float32,
        Float64,
        String>;


class RocksDBDictionary final : public IDictionary
{
public:
    RocksDBDictionary(
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            const DictionaryLifetime dict_lifetime_,
            const std::string & path,
            const size_t max_partitions_count_,
            const size_t partition_size_,
            const size_t block_size_,
            const size_t read_buffer_size_,
            const size_t write_buffer_size_);

    const std::string & getDatabase() const override { return name; }
    const std::string & getName() const override { return name; }
    const std::string & getFullName() const override { return name; }

    std::string getTypeName() const override { return "SSDCache"; }

    size_t getBytesAllocated() const override { return 0; } // TODO: ?

    size_t getQueryCount() const override { return 0; }

    double getHitRate() const override
    {
        return static_cast<double>(0);
    }

    size_t getElementCount() const override { return 0; }

    double getLoadFactor() const override { return 0; }

    bool supportUpdates() const override { return false; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<RocksDBDictionary>(name, dict_struct, source_ptr->clone(), dict_lifetime, path,
                                                    max_partitions_count, partition_size, block_size, read_buffer_size, write_buffer_size);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[getAttributeIndex(attribute_name)].injective;
    }

    bool hasHierarchy() const override { return false; }

    void toParent(const PaddedPODArray<Key> & /* ids */, PaddedPODArray<Key> & /* out */ ) const override {}

    std::exception_ptr getLastException() const override { return last_exception; }

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

#define DECLARE(TYPE) \
void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const;
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

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const;

#define DECLARE(TYPE) \
void get##TYPE( \
    const std::string & attribute_name, \
    const PaddedPODArray<Key> & ids, \
    const PaddedPODArray<TYPE> & def, \
    ResultArrayType<TYPE> & out) const;
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

    void
    getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out)
    const;

#define DECLARE(TYPE) \
void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def, ResultArrayType<TYPE> & out) const;
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

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const;

    void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    size_t getAttributeIndex(const std::string & attr_name) const;

    template <typename T>
    AttributeValueVariant createAttributeNullValueWithTypeImpl(const Field & null_value);
    AttributeValueVariant createAttributeNullValueWithType(const AttributeUnderlyingType type, const Field & null_value);
    void createAttributes();

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
            const size_t attribute_index, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const;

    template <typename DefaultGetter>
    void getItemsString(const size_t attribute_index, const PaddedPODArray<Key> & ids,
                        ColumnString * out, DefaultGetter && get_default) const;

    const std::string name;
    const DictionaryStructure dict_struct;
    mutable DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    const std::string path;
    const size_t max_partitions_count;
    const size_t partition_size;
    const size_t block_size;
    const size_t read_buffer_size;
    const size_t write_buffer_size;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<AttributeValueVariant> null_values;
    std::unique_ptr<rocksdb::DB> db;

    std::exception_ptr last_exception;
    Logger * const log;

    mutable size_t bytes_allocated = 0;
};

}
