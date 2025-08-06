//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  if (n_leading_bits >= 0) {
    dense_bucket_.resize(1 << n_leading_bits);
    n_leading_bits_ = n_leading_bits;
  }
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  const hash_t hash_val = CalculateHash(val);
  std::bitset<64> bs(hash_val);
  size_t bucket_index = (bs >> (64 - n_leading_bits_)).to_ulong();
  std::bitset<64> remian_bs(hash_val - (bucket_index << (64 - n_leading_bits_)));
  int16_t leading_zeros = remian_bs._Find_first();
  u_long new_val = leading_zeros != 64 ? leading_zeros : (64 - n_leading_bits_);
  u_long origin_val = dense_bucket_[bucket_index].to_ulong();
  if (overflow_bucket_.find(bucket_index) != overflow_bucket_.end()) {
    origin_val += (overflow_bucket_[bucket_index].to_ulong()) << DENSE_BUCKET_SIZE;
  }
  u_long target_val = std::max(origin_val, new_val);
  auto overflow_val = (target_val >> DENSE_BUCKET_SIZE);
  if (overflow_val > 0) {
    overflow_bucket_[bucket_index] = overflow_val;
    dense_bucket_[bucket_index] = target_val - (overflow_val << DENSE_BUCKET_SIZE);
    return;
  }
  dense_bucket_[bucket_index] = target_val;
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  int m = dense_bucket_.size();
  if (m == 0) {
    return;
  }
  double sum = 0.0;
  for (int idx = 0; idx < m; ++idx) {
    int64_t val = dense_bucket_[idx].to_ulong();
    if (overflow_bucket_.find(idx) != overflow_bucket_.end()) {
      val += overflow_bucket_[idx].to_ullong() << DENSE_BUCKET_SIZE;
    }
    sum += std::pow(2, -val);
  }
  cardinality_ = std::floor((m * m * CONSTANT) / sum);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
