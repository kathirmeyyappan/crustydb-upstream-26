// pub use sledstore::storage_manager::{StorageManager, STORAGE_DIR};

mod storage_common;
// pub use memstore::storage_manager::{StorageManager, STORAGE_DIR};
// Output when hs in list (so PG strip does not re-export missing StorageManager)
pub use heapstore::storage_manager::{StorageManager, STORAGE_DIR};
