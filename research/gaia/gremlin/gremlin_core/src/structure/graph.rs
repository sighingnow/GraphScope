//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::generated::common as pb_common;
use crate::generated::gremlin as pb;
use crate::structure::{Direction, Edge, ElementFilter, Filter, Label, PropKey, Vertex, ID};
use crate::{DynIter, DynResult, Element, FromPb};

#[derive(Clone)]
pub struct QueryParams<E: Element + Send + Sync> {
    pub labels: Vec<Label>,
    pub limit: Option<usize>,
    pub props: Option<Vec<PropKey>>,
    pub filter: Option<Arc<Filter<E, ElementFilter>>>,
    pub extra_params: Option<HashMap<String, Object>>,
}

impl<E: Element + Send + Sync> Default for QueryParams<E> {
    fn default() -> Self {
        QueryParams { labels: vec![], limit: None, props: None, filter: None, extra_params: None }
    }
}

impl<E: Element + Send + Sync> QueryParams<E> {
    pub fn set_filter(&mut self, filter: Filter<E, ElementFilter>) {
        self.filter = Some(Arc::new(filter))
    }

    // props specify the properties we query for, e.g.,
    // Some(vec![prop1, prop2]) indicates we need prop1 and prop2,
    // Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property,
    pub fn set_props(&mut self, required_properties: Option<pb::PropKeys>) {
        if let Some(fetch_props) = required_properties {
            let mut prop_keys = vec![];
            for prop_key in fetch_props.prop_keys {
                if let Ok(prop_key) = PropKey::from_pb(prop_key) {
                    prop_keys.push(prop_key);
                } else {
                    debug!("Parse prop key failed");
                }
            }
            // the cases of we need all properties or some specific properties
            if fetch_props.is_all || !prop_keys.is_empty() {
                self.props = Some(prop_keys)
            }
        }
    }

    // Extra query params for different storages
    pub fn set_extra_params(&mut self, extra_params_pb: Option<pb::ExtraParams>) {
        if let Some(extra_params_pb) = extra_params_pb {
            let mut extra_params = HashMap::new();
            for param in extra_params_pb.params {
                let param_value = match param.value.unwrap().item.unwrap() {
                    pb_common::value::Item::Boolean(b) => b.into(),
                    pb_common::value::Item::I32(i) => i.into(),
                    pb_common::value::Item::I64(i) => i.into(),
                    pb_common::value::Item::F64(f) => f.into(),
                    pb_common::value::Item::Str(s) => s.into(),
                    pb_common::value::Item::Blob(b) => b.into(),
                    _ => {
                        unimplemented!()
                    }
                };
                extra_params.insert(param.key, param_value);
            }
            self.extra_params = Some(extra_params);
        }
    }

    pub fn get_extra_param(&self, key: &str) -> Option<&Object> {
        if let Some(ref extra_params) = self.extra_params {
            extra_params.get(key)
        } else {
            None
        }
    }
}

pub trait Statement<I, O>: Send + 'static {
    fn exec(&self, next: I) -> DynResult<DynIter<O>>;
}

impl<I, O, F: 'static> Statement<I, O> for F
where
    F: Fn(I) -> DynResult<DynIter<O>> + Send + Sync,
{
    fn exec(&self, param: I) -> DynResult<DynIter<O>> {
        (self)(param)
    }
}

pub trait GraphProxy: Send + Sync {
    fn scan_vertex(
        &self, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>>;

    fn scan_edge(
        &self, params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>>;

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>>;

    fn get_edge(
        &self, ids: &[ID], params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>>;

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Statement<ID, Vertex>>>;

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Statement<ID, Edge>>>;
}

use dyn_type::Object;
use std::collections::HashMap;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

lazy_static! {
    pub static ref GRAPH_PROXY: AtomicPtr<Arc<dyn GraphProxy>> = AtomicPtr::default();
}

pub fn register_graph(graph: Arc<dyn GraphProxy>) {
    let ptr = Box::into_raw(Box::new(graph));
    GRAPH_PROXY.store(ptr, Ordering::SeqCst);
}

pub fn get_graph() -> Option<Arc<dyn GraphProxy>> {
    let ptr = GRAPH_PROXY.load(Ordering::SeqCst);
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { (*ptr).clone() })
    }
}
