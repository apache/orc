#ifndef ORC_SCHEMA_EVOLUTION_HH
#define ORC_SCHEMA_EVOLUTION_HH

#include "orc/Type.hh"

#include <unordered_map>
#include <unordered_set>

namespace orc {

  /**
   * Utility class to compare read type and file type to match their columns
   * and check type conversion.
   */
  class SchemaEvolution {
   public:
    SchemaEvolution(const std::shared_ptr<Type>& readType, const Type* fileType);

    // get read type by column id from file type. or return the file type if
    // read type is not provided (i.e. no schema evolution requested).
    const Type* getReadType(const Type& fileType) const;

    // check if we need to convert file type to read type for primitive type.
    bool needConvert(const Type& fileType) const;

    // check if the PPD conversion is safe
    bool isSafePPDConversion(uint64_t columnId) const;

    // return selected read type
    const Type* getReadType() const {
      return readType.get();
    }

   private:
    void buildConversion(const Type* readType, const Type* fileType);
    void buildSafePPDConversionMap(const Type* readType, const Type* fileType);

   private:
    const std::shared_ptr<Type> readType;
    std::unordered_map<uint64_t, const Type*> readTypeMap;
    std::unordered_set<uint64_t> safePPDConversionMap;
  };

}  // namespace orc

#endif  // ORC_SCHEMA_EVOLUTION_HH
