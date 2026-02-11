/*
 * Reference: https://github.com/yongwen/columbia/blob/master/header/supp.h
 */

use std::{fmt::Debug, ops::Add};

use common::ids::GroupId;
use queryexe::query::translate_and_validate::Query;

// When not building for qo: alias (DummyMemoNode defined below in same module).
// Main repo: cfg hides alias so only except="qo" block compiles. Strip output: cfg line removed, alias kept, so out/ needs no feature.
pub type MemoNodeRefWrapper<C> = DummyMemoNode<C>;

pub mod dummy_cost_model;

pub trait Cost: Default + Clone + PartialEq + PartialOrd + Debug + Add<Output = Self> {}

pub trait CostModel: Clone {
    type Cost: Cost;

    fn set_up(&mut self, query: &Query);

    fn calculate_cost<C: Cost>(&mut self, gid: GroupId, expr: MemoNodeRefWrapper<C>) -> Self::Cost;

    fn get_zero_cost(&self) -> Self::Cost;
}

#[allow(dead_code)]
pub struct DummyMemoNode<C: Cost> {
    pub cost: C,
}
