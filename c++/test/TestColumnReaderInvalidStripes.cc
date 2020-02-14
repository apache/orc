
#include "Adaptor.hh"
#include "ColumnReader.hh"
#include "orc/Exceptions.hh"
#include "OrcTest.hh"

#include "wrap/orc-proto-wrapper.hh"
#include "wrap/gtest-wrapper.h"
#include "wrap/gmock.h"

#include <cmath>
#include <iostream>
#include <vector>

#ifdef __clang__
  DIAGNOSTIC_IGNORE("-Winconsistent-missing-override")
  DIAGNOSTIC_IGNORE("-Wmissing-variable-declarations")
#endif
#ifdef __GNUC__
  DIAGNOSTIC_IGNORE("-Wparentheses")
#endif

namespace orc {
  using ::testing::TestWithParam;
  using ::testing::Values;

  class TestColumnReaderInvalidStripes : public ::testing::Test {
    class MockStripeStreams : public StripeStreams {
    public:
      virtual ~MockStripeStreams() override;

      std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId,
                                                    proto::Stream_Kind kind,
                                                    bool stream)
                                                    const override {
      return std::unique_ptr<SeekableInputStream>
              (getStreamProxy(columnId, kind, stream));
    }

      MOCK_CONST_METHOD0(getSelectedColumns, const std::vector<bool>());
      MOCK_CONST_METHOD1(getEncoding, proto::ColumnEncoding(uint64_t));
      MOCK_CONST_METHOD3(getStreamProxy,
          SeekableInputStream*(uint64_t, proto::Stream_Kind, bool));
      MOCK_CONST_METHOD0(getErrorStream, std::ostream*());
      MOCK_CONST_METHOD0(getThrowOnHive11DecimalOverflow, bool());
      MOCK_CONST_METHOD0(getForcedScaleOnHive11Decimal, int32_t());

      MemoryPool &getMemoryPool() const {
        return *getDefaultPool();
      }

      const Timezone &getWriterTimezone() const override {
        return getTimezoneByName("America/Los_Angeles");
      }
    };

  public:
    virtual ~TestColumnReaderInvalidStripes();
    MockStripeStreams streams;
  protected:
    void SetUp() {
      // set getSelectedColumns()
      std::vector<bool> selectedColumns(2, true);
      EXPECT_CALL(streams, getSelectedColumns())
          .WillRepeatedly(testing::Return(selectedColumns));

      // set getEncoding
      proto::ColumnEncoding directEncoding;
      directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
      EXPECT_CALL(streams, getEncoding(testing::_))
          .WillRepeatedly(testing::Return(directEncoding));

      // set getStream
      EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
          .WillRepeatedly(testing::Return(nullptr));
      const unsigned char buffer1[] = { 0x3d };
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                          (buffer1, ARRAY_SIZE(buffer1))));
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
          .WillRepeatedly(testing::Return(nullptr));
    }

    void TearDown() {
      ::testing::Mock::VerifyAndClearExpectations(&streams);
    }

    void SetNullLengthExpect() {
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
          .WillRepeatedly(testing::Return(nullptr));
    }

    void SetNonNullLengthExpect() {
      const unsigned char buffer4[] =  { 0x02, 0x01, 0x03 };
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                          (buffer4, ARRAY_SIZE(buffer4))));
    }

    void RunTest(std::unique_ptr<Type> type, const char* msg) {
      // create the row type
      std::unique_ptr<Type> rowType = createStructType();
      rowType->addStructField("col0", std::move(type));

      std::unique_ptr<ColumnReader> reader;
      ASSERT_THROW(
        try {
          reader = buildReader(*rowType, streams);
        } catch (ParseError e) {
          EXPECT_STREQ(e.what(), msg);
          throw;
        }, ParseError);
    }
  };

  TestColumnReaderInvalidStripes::MockStripeStreams::~MockStripeStreams() {
    // PASS
  }
  TestColumnReaderInvalidStripes::~TestColumnReaderInvalidStripes(){
    // PASS
  }

TEST_F(TestColumnReaderInvalidStripes, testBool) {
  RunTest(createPrimitiveType(BOOLEAN),
         "DATA stream not found in column 1 of type boolean");
}

TEST_F(TestColumnReaderInvalidStripes, testByte) {
  RunTest(createPrimitiveType(BYTE),
         "DATA stream not found in column 1 of type tinyint");
}

TEST_F(TestColumnReaderInvalidStripes, testInt) {
  RunTest(createPrimitiveType(INT),
         "DATA stream not found in column 1 of type int");
}

TEST_F(TestColumnReaderInvalidStripes, testTimestamp) {
  RunTest(createPrimitiveType(TIMESTAMP),
         "DATA stream not found in column 1 of type timestamp");
}

TEST_F(TestColumnReaderInvalidStripes, testTimestampSecondary) {
  const unsigned char buffer1[] = { 0x3d };
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                          (buffer1, ARRAY_SIZE(buffer1))));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
          .WillRepeatedly(testing::Return(nullptr));
  RunTest(createPrimitiveType(TIMESTAMP),
         "SECONDARY stream not found in column 1 of type timestamp");
}

TEST_F(TestColumnReaderInvalidStripes, testDouble) {
  RunTest(createPrimitiveType(DOUBLE),
         "DATA stream not found in column 1 of type double");
}

// Length stream is called first.
TEST_F(TestColumnReaderInvalidStripes, testStringLength) {
  SetNullLengthExpect();
  RunTest(createPrimitiveType(STRING),
         "LENGTH stream not found in column 1 of type string");
}

TEST_F(TestColumnReaderInvalidStripes, testStringData) {
  SetNonNullLengthExpect();
  RunTest(createPrimitiveType(STRING),
         "DATA stream not found in column 1 of type string");
}

// Lists have only length streams.
TEST_F(TestColumnReaderInvalidStripes, testList) {
  SetNullLengthExpect();
  RunTest(createListType(createPrimitiveType(INT)),
         "LENGTH stream not found in column 1 of type array<int>");
}

// Maps have only length streams.
TEST_F(TestColumnReaderInvalidStripes, testMap) {
  SetNullLengthExpect();
  RunTest(createMapType(createPrimitiveType(INT), createPrimitiveType(INT)),
         "LENGTH stream not found in column 1 of type map<int,int>");
}

TEST_F(TestColumnReaderInvalidStripes, testUnion) {
  RunTest(createUnionType(),
         "DATA stream not found in column 1 of type uniontype<>");
}

TEST_F(TestColumnReaderInvalidStripes, testDecimalData) {
  RunTest(createDecimalType(4,2),
         "DATA stream not found in column 1 of type decimal(4,2)");
}

TEST_F(TestColumnReaderInvalidStripes, testDecimalSecondary) {
  const unsigned char buffer1[] = { 0x3d };
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                          (buffer1, ARRAY_SIZE(buffer1))));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
          .WillRepeatedly(testing::Return(nullptr));
  RunTest(createDecimalType(4,2),
         "SECONDARY stream not found in column 1 of type decimal(4,2)");
}

}  // namespace orc
