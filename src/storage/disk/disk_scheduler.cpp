//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "storage/disk/disk_scheduler.h"
#include <functional>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  for (size_t i = 0; i < pool_size_; i++) {
    request_queues_.emplace_back(std::make_shared<Chan>());
  }
  for (size_t i = 0; i < pool_size_; i++) {
    background_threads_.emplace_back([this, idx = i] { StartWorkerThread(request_queues_[idx]); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (size_t i = 0; i < pool_size_; i++) {
    request_queues_[i]->Put(std::nullopt);
  }
  for (size_t i = 0; i < pool_size_; i++) {
    background_threads_[i].join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) {
  size_t hasher = std::hash<size_t>{}(r.page_id_);
  request_queues_[hasher % pool_size_]->Put(std::move(r));
}

void DiskScheduler::ProcessDiskRequest(DiskRequest r) {
  if (r.pre_cond_.has_value()) {
    r.pre_cond_.value().get();
  }
  if (r.is_write_) {
    disk_manager_->WritePage(r.page_id_, r.data_);
  } else {
    disk_manager_->ReadPage(r.page_id_, r.data_);
  }
  r.callback_.set_value(true);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread(const std::shared_ptr<Chan> &chan) {
  std::optional<DiskRequest> r;
  while ((r = chan->Get()) != std::nullopt) {
    ProcessDiskRequest(std::move(r.value()));
  }
}

}  // namespace bustub
