//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  if (n_bits >= 0) {
    n_bits_ = n_bits;
    registers_.resize(1 << n_bits, 0);
  }
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  std::bitset<BITSET_CAPACITY> bs(hash);
  return bs;
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  size_t position = BITSET_CAPACITY;
  for (int i = bset.size() - 1; i >= 0; --i) {
    if (bset[i]) {
      position = (bset.size() - 1 - i);
      break;
    }
  }
  return position;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  const hash_t hash_val = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bs = ComputeBinary(hash_val);
  size_t bucket_index = (bs >> (BITSET_CAPACITY - n_bits_)).to_ulong();
  int16_t leading_zeros = PositionOfLeftmostOne(bs << n_bits_);
  if (leading_zeros == BITSET_CAPACITY) {
    std::cout << "bs:" << (bs << n_bits_) << " leading_zeros:" << leading_zeros << std::endl;
  }
  int16_t target_val = leading_zeros != BITSET_CAPACITY ? leading_zeros + 1 : (BITSET_CAPACITY - n_bits_) + 1;
  registers_[bucket_index] = std::max(registers_[bucket_index], target_val);
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  int m = registers_.size();
  if (m == 0) {
    return;
  }
  double sum = 0.0;
  for (int i = 0; i < m; ++i) {
    sum += std::pow(2, -registers_[i]);
  }
  cardinality_ = std::floor((m * m * CONSTANT) / sum);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
