#!/bin/bash

cd .deps
test -f ragel-6.10.tar.gz || {
  curl -LO http://www.colm.net/files/ragel/ragel-6.10.tar.gz
  tar xzvf ragel-6.10.tar.gz
  cd ragel-6.10
  ./configure && make && make install
  cd -
}

test -f boost_1_73_0.tar.gz || {
  curl -LO https://dl.bintray.com/boostorg/release/1.73.0/source/boost_1_73_0.tar.gz
  tar xzvf boost_1_73_0.tar.gz
}

test -f hyperscan-5.2.1.tar.gz || {
  curl -L https://github.com/intel/hyperscan/archive/v5.2.1.tar.gz -o hyperscan-5.2.1.tar.gz
  tar xzvf hyperscan-5.2.1.tar.gz
  ln -sf boost_1_73_0/boost/ hyperscan-5.2.1/include/boost
  mkdir hyperscan-5.2.1/build
  cd hyperscan-5.2.1/build
  cmake .. && make && make install
  cd -
}
