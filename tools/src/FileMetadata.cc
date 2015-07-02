/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <iomanip>

#include "wrap/orc-proto-wrapper.hh"
#include "orc/OrcFile.hh"

using namespace orc::proto;

uint64_t getTotalPaddingSize(Footer footer);

int main(int argc, char* argv[])
{
  std::ifstream input;

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (argc < 2) {
    std::cout << "Usage: file-metadata <filename>\n";
  }

  std::cout << "Structure for " << argv[1] << std::endl;

  input.open(argv[1], std::ios::in | std::ios::binary);
  input.seekg(0,input.end);
  std::streamoff fileSize = input.tellg();

  // Read the postscript size
  input.seekg(fileSize-1);
  int result = input.get();
  if (result == EOF) {
    std::cerr << "Failed to read postscript size\n";
    return -1;
  }
  std::streamoff postscriptSize = result;

  // Read the postscript
  input.seekg(fileSize - postscriptSize-1);
  std::vector<char> buffer(static_cast<size_t>(postscriptSize));
  input.read(buffer.data(), postscriptSize);
  PostScript postscript ;
  postscript.ParseFromArray(buffer.data(),
                            static_cast<int>(postscriptSize));
  std::cout << std::endl << " === Postscript === " << std::endl ;
  postscript.PrintDebugString();

  // Everything but the postscript is compressed
  switch (static_cast<int>(postscript.compression())) {
  case NONE:
      break;
  case ZLIB:
  case SNAPPY:
  case LZO:
  default:
      std::cout << "ORC files with compression are not supported" << std::endl ;
      input.close();
      return -1;
  };

  std::streamoff footerSize =
    static_cast<std::streamoff>(postscript.footerlength());
  std::streamoff metadataSize =
    static_cast<std::streamoff>(postscript.metadatalength());

  // Read the metadata
  input.seekg(fileSize - 1 - postscriptSize - footerSize - metadataSize);
  buffer.resize(static_cast<size_t>(metadataSize));
  input.read(buffer.data(), metadataSize);
  Metadata metadata ;
  metadata.ParseFromArray(buffer.data(), static_cast<int>(metadataSize));

  // Read the footer
  //input.seekg(fileSize -1 - postscriptSize-footerSize);
  buffer.resize(static_cast<size_t>(footerSize));
  input.read(buffer.data(), footerSize);
  Footer footer ;
  footer.ParseFromArray(buffer.data(), static_cast<int>(footerSize));
  std::cout << std::endl << " === Footer === " << std::endl ;
  footer.PrintDebugString();

  std::cout << std::endl << "=== Stripe Statistics ===" << std::endl;

  StripeInformation stripe ;
  Stream section;
  ColumnEncoding encoding;
  for (int stripeIx=0; stripeIx<footer.stripes_size(); stripeIx++)
  {
      std::cout << "Stripe " << stripeIx+1 <<": " << std::endl ;
      stripe = footer.stripes(stripeIx);
      stripe.PrintDebugString();

      std::streamoff offset =
        static_cast<std::streamoff>(stripe.offset() + stripe.indexlength() +
                                    stripe.datalength());
      std::streamoff tailLength =
        static_cast<std::streamoff>(stripe.footerlength());

      // read the stripe footer
      input.seekg(offset);
      buffer.resize(static_cast<size_t>(tailLength));
      input.read(buffer.data(), tailLength);

      StripeFooter stripeFooter;
      stripeFooter.ParseFromArray(buffer.data(), static_cast<int>(tailLength));
      //stripeFooter.PrintDebugString();
      uint64_t stripeStart = stripe.offset();
      uint64_t sectionStart = stripeStart;
      for (int streamIx=0; streamIx<stripeFooter.streams_size(); streamIx++) {
          section = stripeFooter.streams(streamIx);
          std::cout << "    Stream: column " << section.column()
                    << " section "
                    << section.kind() << " start: " << sectionStart
                    << " length " << section.length() << std::endl;
          sectionStart += section.length();
      };
      for (int columnIx=0; columnIx<stripeFooter.columns_size();
           columnIx++) {
          encoding = stripeFooter.columns(columnIx);
          std::cout << "    Encoding column " << columnIx << ": "
                    << encoding.kind() ;
          if (encoding.kind() == ColumnEncoding_Kind_DICTIONARY ||
              encoding.kind() == ColumnEncoding_Kind_DICTIONARY_V2)
              std::cout << "[" << encoding.dictionarysize() << "]";
          std::cout << std::endl;
      };
  };

  uint64_t paddedBytes = getTotalPaddingSize(footer);
  // empty ORC file is ~45 bytes. Assumption here is file length always >0
  double percentPadding =
    static_cast<double>(paddedBytes) * 100 / static_cast<double>(fileSize);
  std::cout << "File length: " << fileSize << " bytes" << std::endl;
  std::cout <<"Padding length: " << paddedBytes << " bytes" << std::endl;
  std::cout <<"Padding ratio: " << std::fixed << std::setprecision(2)
            << percentPadding << " %" << std::endl;

  input.close();



  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}

uint64_t getTotalPaddingSize(Footer footer) {
  uint64_t paddedBytes = 0;
  StripeInformation stripe;
  for (int stripeIx=1; stripeIx<footer.stripes_size(); stripeIx++) {
      stripe = footer.stripes(stripeIx-1);
      uint64_t prevStripeOffset = stripe.offset();
      uint64_t prevStripeLen = stripe.datalength() + stripe.indexlength() +
        stripe.footerlength();
      paddedBytes += footer.stripes(stripeIx).offset() -
        (prevStripeOffset + prevStripeLen);
  };
  return paddedBytes;
}


