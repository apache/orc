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

#include "orc/ColumnPrinter.hh"
#include "Exceptions.hh"

#include <string>
#include <memory>
#include <iostream>
#include <string>

void printStatistics(const char *filename) {

  orc::ReaderOptions opts;
  std::unique_ptr<orc::Reader> reader;
  reader = orc::createReader(orc::readLocalFile(std::string(filename)), opts);

  // print out all selected columns statistics.
  std::unique_ptr<orc::Statistics> colStats = reader->getStatistics();
  std::cout << "File " << filename << " has "
            << colStats->getNumberOfColumns() << " columns"  << std::endl;
  for(uint32_t i=0; i < colStats->getNumberOfColumns(); ++i) {
    std::cout << "*** Column " << i << " ***" << std::endl;
    std::cout << colStats->getColumnStatistics(i)->toString() << std::endl;
  }

  // test stripe statistics
  std::unique_ptr<orc::Statistics> stripeStats;
  std::cout << "File " << filename << " has " << reader->getNumberOfStripes()
            << " stripes"  << std::endl;
  if (reader->getNumberOfStripeStatistics() == 0) {
    std::cout << "File " << filename << " doesn't have stripe statistics"
              << std::endl;
  } else {
    for (unsigned int j = 0; j < reader->getNumberOfStripeStatistics(); j++) {
      stripeStats = reader->getStripeStatistics(j);
      std::cout << "*** Stripe " << j << " ***" << std::endl << std::endl;

      for(unsigned int k = 0; k < stripeStats->getNumberOfColumns(); ++k) {
        std::cout << "--- Column " << k << " ---" << std::endl;
        std::cout << stripeStats->getColumnStatistics(k)->toString()
                  << std::endl;
      }
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: orc-statistics <filename>\n";
  }

  try {
    printStatistics(argv[1]);
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }

  return 0;
}
