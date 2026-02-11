use common::prelude::*;
#[allow(unused_imports)]
use common::PAGE_SIZE;

#[allow(unused_imports)]
use crate::page::{Offset, Page, OFFSET_NUM_BYTES};

use std::mem;
use crate::page::{Slot, PAGE_FIXED_HEADER_LEN, SLOT_EMPTY};
use std::convert::TryInto;

const SLOT_SIZE: usize = mem::size_of::<Slot>();

#[allow(dead_code)]
/// The size of a slotID
pub(crate) const SLOT_ID_SIZE: usize = mem::size_of::<SlotId>();
#[allow(dead_code)]
/// The allowed metadata size per slot
pub(crate) const SLOT_METADATA_SIZE: usize = 4;
#[allow(dead_code)]
/// The size of the metadata allowed for the heap page, this is in addition to the page header
pub(crate) const HEAP_PAGE_FIXED_METADATA_SIZE: usize = 8;

/// This is trait of a HeapPage for the Page struct.
///
/// The page header size is fixed to `PAGE_FIXED_HEADER_LEN` bytes and you will use
/// additional bytes for the HeapPage metadata
/// Your HeapPage implementation can use a fixed metadata of 8 bytes plus 4 bytes per value/entry/slot stored.
/// For example a page that has stored 3 values, we would assume that the fist
/// `PAGE_FIXED_HEADER_LEN` bytes are used for the page metadata, 8 bytes for the HeapPage metadata
/// and 12 bytes for slot meta data (4 bytes for each of the 3 values).
/// This leave the rest free for storing data (PAGE_SIZE-PAGE_FIXED_HEADER_LEN-8-12).
///
/// If you delete a value, you do not need reclaim header space the way you must reclaim page
/// body space. E.g., if you insert 3 values then delete 2 of them, your header can remain 26
/// bytes & subsequent inserts can simply add 6 more bytes to the header as normal.
/// The rest must filled as much as possible to hold values.
pub trait HeapPage {
    // Add any new functions here
    fn find_existing_space_to_reclaim(&self, need: usize) -> Option<Offset>;
    fn find_free_space_or_compact(
        &mut self,
        need: usize,
        space_for_header: usize,
    ) -> Option<Offset>;
    fn compact_space(&mut self);
    fn get_slot_len(&self) -> usize;
    fn get_slot(&self, slot_id: SlotId) -> Option<Slot>;
    fn get_first_empty_slot(&self) -> Option<SlotId>;
    fn get_free_marker(&self) -> usize;
    fn set_slot(&mut self, slot_id: SlotId, slot: Slot);
    fn add_slot(&mut self, slot: Slot) -> SlotId;
    fn get_slots(&self) -> Vec<Slot>;
    fn num_slots(&self) -> usize;


    // Do not change these functions signatures (only the function bodies)

    /// Initialize the page struct as a heap page.
    #[allow(dead_code)]
    fn init_heap_page(&mut self);

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should reuse the slotId in the future.
    /// The page should always assign the lowest available slot_id to an insertion.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    #[allow(dead_code)]
    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId>;

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    #[allow(dead_code)]
    fn get_value(&self, slot_id: SlotId) -> Option<&[u8]>;

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    #[allow(dead_code)]
    fn delete_value(&mut self, slot_id: SlotId) -> Option<()>;

    /// Update the value for the slotId. If the slotId is not valid or there is not
    /// space on the page return None and leave the old value/slot. If there is space, update the value and return Some(())
    #[allow(dead_code)]
    fn update_value(&mut self, slot_id: SlotId, bytes: &[u8]) -> Option<()>;

    /// A utility function to determine the current size of the header for this page
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    fn get_header_size(&self) -> usize;

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests. Optional for you to use in your code, but strongly suggested
    #[allow(dead_code)]
    fn get_free_space(&self) -> usize;

    #[allow(dead_code)]
    /// Create an iterator for the page. This should return an iterator that will
    /// return the bytes and the slotId for each value in the page.
    fn iter(&self) -> HeapPageIter<'_>;
}

impl HeapPage for Page {
    fn num_slots(&self) -> usize {
        self.get_slot_len()
    }

    fn find_existing_space_to_reclaim(&self, need: usize) -> Option<Offset> {
        // Reverse sort slots by offsets
        let mut used_offset_reverse = self.get_slots();
        used_offset_reverse.sort_by(|a, b| b.0.cmp(&a.0));

        // Look at difference between window pairs to see if there is old room.
        // 90,10   80,10, 70,5

        for window in used_offset_reverse.windows(2) {
            let space = window[0].0 - (window[1].0 + window[1].1);
            if space >= need as Offset {
                return Some(window[0].0 - need as Offset);
            }
        }
        None
    }

    fn find_free_space_or_compact(
        &mut self,
        need: usize,
        space_for_header: usize,
    ) -> Option<Offset> {
        trace!(
            "finding free space. need for value: {} need space for header: {}",
            need,
            space_for_header
        );
        // Check for gaps in the tail first.
        let reclaimable_slot = self.find_existing_space_to_reclaim(need);
        if reclaimable_slot.is_some() {
            if self.get_header_size() + space_for_header >= self.get_free_marker() {
                // If this is a new slot need to make sure the extra slot
                // won't overflow the required space
                return None;
            } else {
                return reclaimable_slot;
            }
        }

        let free_space = self.get_free_space();

        if free_space <= (space_for_header + need) {
            trace!("Not enough space");
            None
        } else {
            trace!(
                "Free space {}. Space for header needed: {}. Needed for data {}",
                free_space,
                space_for_header,
                need
            );
            let free_marker = self.get_free_marker();
            if self.get_header_size() + space_for_header + need < free_marker {
                // Do we have space after the header before the free marker? if so just put it there
                let new_free = free_marker - need;
                trace!("Adding value to free space. Header Size:{} Header Needed: {} Space Need: {} New Free / Slot:{}", self.get_header_size(), space_for_header, need, new_free);
                Some(new_free as SlotId)
            } else {
                //trace!("Pre compact {:?}", self);
                self.compact_space();
                //trace!("post compact {:?}", self);
                let new_free = self.get_free_marker() - need;
                Some(new_free as SlotId)
            }
        }
    }

    // Dirty way of compacting. Just make a new copy of things and smash the values together
    fn compact_space(&mut self) {
        trace!("Compacting space");
        let old_slots = self.get_slots();
        let old_page = self.data; // should call copy
                                  //Zero out the existing data array
                                  // TODO this would wipe everything, needs to start from header
                                  // self.data[PAGE_FIXED_HEADER_LEN+1..].iter_mut().for_each(|m| *m = 0);
        let mut pos = PAGE_SIZE;
        for (slot_id, (old_offset, len)) in old_slots.iter().enumerate() {
            pos -= *len as usize;
            let new_slot = (pos as Offset, *len);
            let old_bytes = &old_page[*old_offset as usize..(old_offset + len) as usize];
            let mut preview_bytes: usize = 12;
            if *len > 0 {
                self.data[pos..pos + *len as usize].clone_from_slice(old_bytes);
            }
            if (*len as usize) < preview_bytes {
                preview_bytes = *len as usize;
            }
            self.set_slot(slot_id as SlotId, new_slot);
            trace!(
                "Moving ({}) {} to {}, data: {:?}",
                len,
                old_offset,
                pos,
                old_bytes[..preview_bytes].to_vec()
            )
        }
        trace!("Done compacting space");
    }

    #[inline(always)]
    fn get_slot_len(&self) -> usize {
        SlotId::from_le_bytes(
            self.data[PAGE_FIXED_HEADER_LEN..PAGE_FIXED_HEADER_LEN + SLOT_ID_SIZE]
                .try_into()
                .expect("Error getting slot count"),
        ) as usize
    }

    #[inline(always)]
    fn get_slot(&self, slot_id: SlotId) -> Option<Slot> {
        if slot_id as usize >= self.get_slot_len() {
            None
        } else {
            let start = PAGE_FIXED_HEADER_LEN + SLOT_ID_SIZE + (slot_id as usize * SLOT_SIZE);
            let offset = Offset::from_le_bytes(
                self.data[start..start + SLOT_ID_SIZE]
                    .try_into()
                    .expect("Error getting slot offset"),
            );
            let len = Offset::from_le_bytes(
                self.data[start + SLOT_ID_SIZE..start + SLOT_ID_SIZE + OFFSET_NUM_BYTES]
                    .try_into()
                    .expect("Error getting slot len"),
            );
            Some((offset, len))
        }
    }

    #[inline(always)]
    fn get_slots(&self) -> Vec<Slot> {
        let mut slots = Vec::new();
        let slot_len = self.get_slot_len() as Offset;
        for slot_id in 0..slot_len {
            let slot = self.get_slot(slot_id).expect("Slot was invalid");
            slots.push(slot);
        }
        slots
    }

    #[inline(always)]
    /// Check if there is an empty (existing) slot in the page
    fn get_first_empty_slot(&self) -> Option<SlotId> {
        let slots = self.get_slot_len() as SlotId;
        if slots == 0 {
            return None;
        }
        for slot_id in 0..slots {
            let slot = self.get_slot(slot_id).expect("Slot was invalid");
            if slot.0 != 0 && slot.1 == 0 {
                return Some(slot_id);
            }
        }
        None
    }

    #[inline(always)]
    /// Find the smallest offset to find where the contiguous free space starts
    /// we fill back to front, so this is the smallest offset
    fn get_free_marker(&self) -> usize {
        let slots = self.get_slot_len() as SlotId;
        let mut free_spot = PAGE_SIZE;
        for slot_id in 0..slots {
            let slot_offset = self.get_slot(slot_id).expect("Slot was invalid").0 as usize;
            if slot_offset < free_spot {
                free_spot = slot_offset;
            }
        }
        free_spot
    }

    #[inline(always)]
    fn set_slot(&mut self, slot_id: SlotId, slot: Slot) {
        let mut slot_bytes = [0u8; SLOT_SIZE];
        let (left, right) = slot_bytes.split_at_mut(OFFSET_NUM_BYTES);
        left.copy_from_slice(&slot.0.to_le_bytes());
        right.copy_from_slice(&slot.1.to_le_bytes());
        let start = PAGE_FIXED_HEADER_LEN + SLOT_ID_SIZE + (slot_id as usize * SLOT_SIZE);
        self.data[start..start + SLOT_SIZE].clone_from_slice(&slot_bytes);
    }

    #[inline(always)]
    fn add_slot(&mut self, slot: Slot) -> SlotId {
        let mut slot_bytes = [0u8; SLOT_SIZE];
        let (left, right) = slot_bytes.split_at_mut(OFFSET_NUM_BYTES);
        left.copy_from_slice(&slot.0.to_le_bytes());
        right.copy_from_slice(&slot.1.to_le_bytes());
        let mut slot_len = self.get_slot_len();
        let new_slot_id = slot_len as SlotId;
        let start = PAGE_FIXED_HEADER_LEN + SLOT_ID_SIZE + (slot_len * SLOT_SIZE);

        // write slot
        self.data[start..start + SLOT_SIZE].clone_from_slice(&slot_bytes);

        //write updated slot_len
        slot_len += 1;
        let slot_len_bytes = (slot_len as SlotId).to_le_bytes();
        self.data[PAGE_FIXED_HEADER_LEN..PAGE_FIXED_HEADER_LEN + SLOT_ID_SIZE]
            .clone_from_slice(&slot_len_bytes);

        new_slot_id
    }


    fn init_heap_page(&mut self) {
        //TODO milestone pg
        //Add any initialization code here
    }

    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        panic!("TODO milestone pg");
    }

    fn get_value(&self, slot_id: SlotId) -> Option<&[u8]> {
        panic!("TODO milestone pg");
    }

    fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        panic!("TODO milestone pg");
    }

    fn update_value(&mut self, slot_id: SlotId, bytes: &[u8]) -> Option<()> {
        panic!("TODO milestone pg");
    }

    #[allow(dead_code)]
    fn get_header_size(&self) -> usize {
        panic!("TODO milestone pg");
    }

    #[allow(dead_code)]
    fn get_free_space(&self) -> usize {
        panic!("TODO milestone pg");
    }

    fn iter(&self) -> HeapPageIter<'_> {
        HeapPageIter {
            page: self,
            //TODO milestone pg
            //Initialize with added variables here
        }
    }
}

pub struct HeapPageIter<'a> {
    page: &'a Page,
    //TODO milestone pg
    // Add any variables here
}

impl<'a> Iterator for HeapPageIter<'a> {
    type Item = (&'a [u8], SlotId);

    /// This function will return the next value in the page. It should return
    /// None if there are no more values in the page.
    /// The iterator should return the bytes reference and the slotId for each value in the page as a tuple.
    fn next(&mut self) -> Option<Self::Item> {
        panic!("TODO milestone pg");
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl<'a> IntoIterator for &'a Page {
    type Item = (&'a [u8], SlotId);
    type IntoIter = HeapPageIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        HeapPageIter {
            page: self,
            //TODO milestone pg
            //Initialize with added variables here
        }
    }
}
