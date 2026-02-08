use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::Py;
use tokio::runtime::Runtime;

type PyObject = Py<pyo3::PyAny>;

use crate::canary::utility::protobuf_shared_types::variant::Kind;
use crate::canary::views::grpc::api::*;

// ---------------------------------------------------------------------------
// Helpers for converting protobuf types to Python
// ---------------------------------------------------------------------------

fn variant_to_py(py: Python<'_>, v: &crate::canary::utility::protobuf_shared_types::Variant) -> PyObject {
    match &v.kind {
        Some(Kind::Bool(b)) => b.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::Int8(i)) => i.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::Int16(i)) => i.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::Int32(i)) => i.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::Int64(i)) => i.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::UInt8(u)) => u.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::UInt16(u)) => u.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::UInt32(u)) => u.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::UInt64(u)) => u.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::Float(f)) => f.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::Double(d)) => d.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::String(s)) => s.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Some(Kind::Decimal(b)) => b.as_slice().into_pyobject(py).unwrap().into_any().unbind(),
        None => py.None(),
    }
}

fn timestamp_to_iso(ts: &prost_types::Timestamp) -> String {
    let secs = ts.seconds;
    let nanos = ts.nanos as u64;
    // Format as ISO 8601 with nanoseconds
    let dt_secs = secs;
    let (days_from_epoch, time_secs) = (dt_secs / 86400, dt_secs % 86400);
    if time_secs < 0 || days_from_epoch < 0 {
        return format!("{}s{}ns", secs, nanos);
    }
    // Simple epoch-based formatting
    let hours = time_secs / 3600;
    let mins = (time_secs % 3600) / 60;
    let s = time_secs % 60;

    // Days since 1970-01-01
    let mut days = days_from_epoch;
    let mut year = 1970i64;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let leap = is_leap(year);
    let month_days: [i64; 12] = [
        31, if leap { 29 } else { 28 }, 31, 30, 31, 30,
        31, 31, 30, 31, 30, 31,
    ];
    let mut month = 0usize;
    for (i, &md) in month_days.iter().enumerate() {
        if days < md {
            month = i;
            break;
        }
        days -= md;
    }
    let day = days + 1;
    if nanos > 0 {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
            year, month + 1, day, hours, mins, s, nanos
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            year, month + 1, day, hours, mins, s
        )
    }
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn tvq_to_py_dict<'py>(py: Python<'py>, tvq: &crate::canary::utility::protobuf_shared_types::GrpcTvq) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    if let Some(ts) = &tvq.timestamp {
        dict.set_item("timestamp", timestamp_to_iso(ts))?;
    } else {
        dict.set_item("timestamp", py.None())?;
    }
    if let Some(v) = &tvq.value {
        dict.set_item("value", variant_to_py(py, v))?;
    } else {
        dict.set_item("value", py.None())?;
    }
    dict.set_item("quality", tvq.quality)?;
    Ok(dict)
}

fn err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------
// Python classes
// ---------------------------------------------------------------------------

/// A Python client for the Canary Views gRPC API.
///
/// Usage:
///     from crowsong import CanaryView
///     view = CanaryView("https://host:55321", "api-key")
///     print(view.get_version())
///     view.disconnect()
#[pyclass]
pub struct CanaryView {
    rt: Runtime,
    client: Option<crate::ViewsClient>,
}

#[pymethods]
impl CanaryView {
    /// Create a new connection to a Canary Views service.
    ///
    /// Args:
    ///     endpoint: The gRPC endpoint URL (e.g. "https://host:55321")
    ///     api_key: The Canary API token
    ///     app: Application name (default: "crowsong")
    ///     user_id: User identifier (default: "python")
    #[new]
    #[pyo3(signature = (endpoint, api_key, app="crowsong", user_id="python"))]
    fn new(endpoint: &str, api_key: &str, app: &str, user_id: &str) -> PyResult<Self> {
        let rt = Runtime::new().map_err(err)?;
        let client = rt
            .block_on(crate::ViewsClient::connect(endpoint, api_key, app, user_id))
            .map_err(err)?;
        Ok(Self {
            rt,
            client: Some(client),
        })
    }

    /// Get the client connection ID.
    fn cci(&self) -> PyResult<i32> {
        Ok(self.client.as_ref().ok_or_else(|| err("disconnected"))?.cci())
    }

    /// Test the gRPC connection.
    fn test(&mut self) -> PyResult<()> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        self.rt.block_on(c.test()).map_err(err)
    }

    /// Send a keepalive for the client connection.
    fn keepalive(&mut self) -> PyResult<()> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        self.rt.block_on(c.keepalive()).map_err(err)
    }

    /// Disconnect from the Canary Views service.
    fn disconnect(&mut self) -> PyResult<()> {
        if let Some(mut client) = self.client.take() {
            self.rt.block_on(client.disconnect()).map_err(err)?;
        }
        Ok(())
    }

    /// Get the service version string.
    fn get_version(&mut self) -> PyResult<String> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_version()).map_err(err)?;
        Ok(resp.version)
    }

    /// Get the list of views accessible to this connection.
    fn get_views(&mut self) -> PyResult<Vec<String>> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_views()).map_err(err)?;
        Ok(resp.views)
    }

    /// Get the datasets for a view.
    ///
    /// Args:
    ///     view: The view name
    ///     include_hidden: Whether to include hidden datasets (default: False)
    #[pyo3(signature = (view, include_hidden=false))]
    fn get_dataset_list(&mut self, view: &str, include_hidden: bool) -> PyResult<Vec<String>> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_dataset_list(view, include_hidden)).map_err(err)?;
        Ok(resp.datasets)
    }

    /// Get dataset info. Returns a dict of property names to values.
    fn get_dataset_info(&mut self, py: Python<'_>, view: &str, dataset_name: &str) -> PyResult<PyObject> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_dataset_info(view, dataset_name)).map_err(err)?;
        let dict = PyDict::new(py);
        for (name, val) in resp.prop_name.iter().zip(resp.prop_value.iter()) {
            dict.set_item(name, val)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Get the tag list for a dataset.
    ///
    /// Args:
    ///     view: The view name
    ///     dataset_name: The dataset name
    ///     starting_offset: Offset to start from (default: 0)
    ///     max_count: Maximum tags to return (default: 10000)
    ///
    /// Returns a list of tag name strings.
    #[pyo3(signature = (view, dataset_name, starting_offset=0, max_count=10000))]
    fn get_tag_list(
        &mut self,
        view: &str,
        dataset_name: &str,
        starting_offset: i32,
        max_count: i32,
    ) -> PyResult<Vec<String>> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self
            .rt
            .block_on(c.get_tag_list(view, dataset_name, starting_offset, max_count))
            .map_err(err)?;
        Ok(resp.tag_names)
    }

    /// Get tag info for specified tags.
    ///
    /// Returns a list of dicts with tag_item_id, item_type, flags, and properties.
    fn get_tag_info(&mut self, py: Python<'_>, view: &str, tag_names: Vec<String>) -> PyResult<PyObject> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_tag_info(view, tag_names)).map_err(err)?;
        let result = PyList::empty(py);
        for info in &resp.tag_infos {
            let d = PyDict::new(py);
            d.set_item("tag_item_id", &info.tag_item_id)?;
            d.set_item("item_type", info.item_type)?;
            d.set_item("flags", info.flags)?;
            let props = PyList::empty(py);
            for p in &info.tag_properties {
                let pd = PyDict::new(py);
                pd.set_item("prop_name", &p.prop_name)?;
                pd.set_item("prop_value", &p.prop_value)?;
                pd.set_item("data_type", &p.data_type)?;
                pd.set_item("prop_description", &p.prop_description)?;
                props.append(pd)?;
            }
            d.set_item("properties", props)?;
            result.append(d)?;
        }
        Ok(result.into_any().unbind())
    }

    /// Get tag data context (temporal bounds) for specified tags.
    ///
    /// Returns a list of dicts with tag_item_id, oldest_timestamp, latest_timestamp, etc.
    fn get_tag_data_context(&mut self, py: Python<'_>, view: &str, tag_names: Vec<String>) -> PyResult<PyObject> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_tag_data_context(view, tag_names)).map_err(err)?;
        let result = PyList::empty(py);
        for ctx in &resp.contexts {
            let d = PyDict::new(py);
            d.set_item("tag_item_id", &ctx.tag_item_id)?;
            if let Some(ts) = &ctx.oldest_timestamp {
                d.set_item("oldest_timestamp", timestamp_to_iso(ts))?;
            }
            if let Some(ts) = &ctx.latest_timestamp {
                d.set_item("latest_timestamp", timestamp_to_iso(ts))?;
            }
            d.set_item("latest_value_data_type", &ctx.latest_value_data_type)?;
            d.set_item("latest_value", &ctx.latest_value)?;
            d.set_item("latest_quality", ctx.latest_quailty)?;
            result.append(d)?;
        }
        Ok(result.into_any().unbind())
    }

    /// Get current values for specified tags.
    ///
    /// Args:
    ///     view: The view name
    ///     tag_names: List of tag names
    ///     quality: Quality filter - "any" (default), "non_bad", or "good"
    ///
    /// Returns a list of dicts with tag_item_id, timestamp, value, quality.
    #[pyo3(signature = (view, tag_names, quality="any"))]
    fn get_tag_current_value(
        &mut self,
        py: Python<'_>,
        view: &str,
        tag_names: Vec<String>,
        quality: &str,
    ) -> PyResult<PyObject> {
        let q = match quality {
            "non_bad" => get_tag_current_value_request::Quality::NonBad,
            "good" => get_tag_current_value_request::Quality::Good,
            _ => get_tag_current_value_request::Quality::Any,
        };
        let req = GetTagCurrentValueRequest {
            view: view.to_string(),
            tag_names,
            use_time_extension: None,
            quality: q.into(),
            cci: 0, // filled in by ViewsClient
        };
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_tag_current_value(req)).map_err(err)?;
        let result = PyList::empty(py);
        for tv in &resp.tag_values {
            let d = PyDict::new(py);
            d.set_item("tag_item_id", &tv.tag_item_id)?;
            if let Some(ts) = &tv.timestamp {
                d.set_item("timestamp", timestamp_to_iso(ts))?;
            }
            if let Some(v) = &tv.value {
                d.set_item("value", variant_to_py(py, v))?;
            } else {
                d.set_item("value", py.None())?;
            }
            d.set_item("quality", tv.quality)?;
            result.append(d)?;
        }
        Ok(result.into_any().unbind())
    }

    /// Get raw historical data for tags.
    ///
    /// Args:
    ///     view: The view name
    ///     tag_names: List of tag names
    ///     start_time: ISO 8601 start timestamp string
    ///     end_time: ISO 8601 end timestamp string
    ///     max_count_per_tag: Max data points per tag (default: 10000)
    ///     return_bounds: Include bounding values (default: False)
    ///
    /// Returns a dict mapping tag_name -> list of {timestamp, value, quality} dicts.
    #[pyo3(signature = (view, tag_names, start_time, end_time, max_count_per_tag=10000, return_bounds=false))]
    fn get_raw_data(
        &mut self,
        py: Python<'_>,
        view: &str,
        tag_names: Vec<String>,
        start_time: &str,
        end_time: &str,
        max_count_per_tag: i32,
        return_bounds: bool,
    ) -> PyResult<PyObject> {
        let start = parse_iso_timestamp(start_time).map_err(err)?;
        let end = parse_iso_timestamp(end_time).map_err(err)?;

        let requests: Vec<RawTagRequest> = tag_names
            .into_iter()
            .map(|tag_name| RawTagRequest {
                tag_name,
                start_time: Some(start.clone()),
                end_time: Some(end.clone()),
                client_data: 0,
                continuation_point: vec![],
            })
            .collect();

        let req = GetRawDataRequest {
            view: view.to_string(),
            requests,
            max_count_per_tag,
            return_bounds,
            return_annotations: false,
            cci: 0,
        };

        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_raw_data(req)).map_err(err)?;

        let result = PyDict::new(py);
        for tag_data in &resp.raw_data {
            let tvqs = PyList::empty(py);
            for tvq in &tag_data.tvqs {
                tvqs.append(tvq_to_py_dict(py, tvq)?)?;
            }
            result.set_item(&tag_data.tag_name, tvqs)?;
        }
        Ok(result.into_any().unbind())
    }

    /// Get aggregated data for tags.
    ///
    /// Args:
    ///     view: The view name
    ///     tag_names: List of tag names
    ///     start_time: ISO 8601 start timestamp string
    ///     end_time: ISO 8601 end timestamp string
    ///     interval_seconds: Aggregation interval in seconds
    ///     aggregate_name: Aggregate function name (e.g. "TimeAverage")
    ///
    /// Returns a dict mapping tag_name -> list of {timestamp, value, quality} dicts.
    #[pyo3(signature = (view, tag_names, start_time, end_time, interval_seconds, aggregate_name="TimeAverage"))]
    fn get_aggregate_data(
        &mut self,
        py: Python<'_>,
        view: &str,
        tag_names: Vec<String>,
        start_time: &str,
        end_time: &str,
        interval_seconds: i64,
        aggregate_name: &str,
    ) -> PyResult<PyObject> {
        let start = parse_iso_timestamp(start_time).map_err(err)?;
        let end = parse_iso_timestamp(end_time).map_err(err)?;

        let requests: Vec<AggregateTagRequest> = tag_names
            .into_iter()
            .map(|tag_name| AggregateTagRequest {
                tag_name,
                aggregate_name: aggregate_name.to_string(),
                aggregate_configuration: None,
                sloped: false,
                client_data: 0,
            })
            .collect();

        let req = GetAggregateDataRequest {
            view: view.to_string(),
            requests,
            start_time: Some(start),
            end_time: Some(end),
            interval: Some(prost_types::Duration {
                seconds: interval_seconds,
                nanos: 0,
            }),
            return_annotations: false,
            cci: 0,
        };

        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_aggregate_data(req)).map_err(err)?;

        let result = PyDict::new(py);
        for tag_data in &resp.aggregated_data {
            let tvqs = PyList::empty(py);
            for tvq in &tag_data.tvqs {
                tvqs.append(tvq_to_py_dict(py, tvq)?)?;
            }
            result.set_item(&tag_data.tag_name, tvqs)?;
        }
        Ok(result.into_any().unbind())
    }

    /// Get available aggregate function names.
    fn get_aggregate_list(&mut self) -> PyResult<Vec<String>> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_aggregate_list()).map_err(err)?;
        Ok(resp
            .aggregates
            .iter()
            .map(|a| a.aggregate_name.clone())
            .collect())
    }

    /// Get tag statistics.
    ///
    /// Args:
    ///     view_name: The view name
    ///     tag_id: The tag ID
    ///     start_time: ISO 8601 start timestamp
    ///     end_time: ISO 8601 end timestamp
    ///     interval_seconds: Interval in seconds
    ///     aggregate_name: Aggregate function (default: "TimeAverage")
    ///     include_std_dev: Include standard deviation (default: True)
    ///     include_percentiles: Include percentiles (default: True)
    ///
    /// Returns a dict with total_samples, valid_samples, sum, mean, minimum,
    /// maximum, standard_dev, percent_25, percent_50, percent_75.
    #[pyo3(signature = (view_name, tag_id, start_time, end_time, interval_seconds, aggregate_name="TimeAverage", include_std_dev=true, include_percentiles=true))]
    fn get_tag_statistics(
        &mut self,
        py: Python<'_>,
        view_name: &str,
        tag_id: &str,
        start_time: &str,
        end_time: &str,
        interval_seconds: i64,
        aggregate_name: &str,
        include_std_dev: bool,
        include_percentiles: bool,
    ) -> PyResult<PyObject> {
        let start = parse_iso_timestamp(start_time).map_err(err)?;
        let end = parse_iso_timestamp(end_time).map_err(err)?;

        let req = GetTagStatisticsRequest {
            view_name: view_name.to_string(),
            tag_id: tag_id.to_string(),
            start_time: Some(start),
            end_time: Some(end),
            interval: Some(prost_types::Duration {
                seconds: interval_seconds,
                nanos: 0,
            }),
            aggregate_name: aggregate_name.to_string(),
            include_std_dev,
            include_percentiles,
            cci: 0,
        };

        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.get_tag_statistics(req)).map_err(err)?;

        let d = PyDict::new(py);
        d.set_item("total_samples", resp.total_samples)?;
        d.set_item("valid_samples", resp.valid_samples)?;
        d.set_item("sum", resp.sum)?;
        d.set_item("mean", resp.mean)?;
        d.set_item("minimum", resp.minimum)?;
        d.set_item("maximum", resp.maximum)?;
        d.set_item("standard_dev", resp.standard_dev)?;
        d.set_item("percent_25", resp.percent_25)?;
        d.set_item("percent_50", resp.percent_50)?;
        d.set_item("percent_75", resp.percent_75)?;
        Ok(d.into_any().unbind())
    }

    /// Browse the views tree by node ID path.
    ///
    /// Returns a dict with parent_id_path and children (list of dicts).
    #[pyo3(signature = (node_id_path="", force_reload=false))]
    fn browse(&mut self, py: Python<'_>, node_id_path: &str, force_reload: bool) -> PyResult<PyObject> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.browse(node_id_path, force_reload)).map_err(err)?;

        let d = PyDict::new(py);
        if let Some(node) = &resp.node {
            d.set_item("parent_id_path", &node.parent_id_path)?;
            d.set_item("error_message", &node.error_message)?;
            let children = PyList::empty(py);
            for child in &node.children {
                let cd = PyDict::new(py);
                cd.set_item("id_path", &child.id_path)?;
                cd.set_item("text", &child.text)?;
                cd.set_item("icon_name", &child.icon_name)?;
                cd.set_item("num_children", child.num_children)?;
                cd.set_item("num_tags", child.num_tags)?;
                children.append(cd)?;
            }
            d.set_item("children", children)?;
        }
        Ok(d.into_any().unbind())
    }

    /// Browse tags at a specified node.
    ///
    /// Args:
    ///     node_id: The node ID to browse
    ///     search_context: Search filter (default: "")
    ///     max_count: Max tags to return (default: 10000)
    ///     include_sub_nodes: Include sub-nodes (default: False)
    ///
    /// Returns a list of tag name strings.
    #[pyo3(signature = (node_id, search_context="", max_count=10000, include_sub_nodes=false))]
    fn browse_tags(
        &mut self,
        node_id: &str,
        search_context: &str,
        max_count: i32,
        include_sub_nodes: bool,
    ) -> PyResult<Vec<String>> {
        let req = BrowseTagsRequest {
            node_id_browse: node_id.to_string(),
            search_context: search_context.to_string(),
            max_count,
            include_sub_nodes,
            include_properties: false,
        };
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.browse_tags(req)).map_err(err)?;
        Ok(resp.tag_names)
    }

    /// Search for tags matching criteria.
    ///
    /// Args:
    ///     tag_and: Tag name must match ALL these patterns
    ///     tag_or: Tag name must match ANY of these patterns
    ///
    /// Returns a list of matching tag name strings.
    #[pyo3(signature = (tag_and=vec![], tag_or=vec![]))]
    fn search_tags(
        &mut self,
        tag_and: Vec<String>,
        tag_or: Vec<String>,
    ) -> PyResult<Vec<String>> {
        let req = SearchTagsRequest {
            tag_and,
            tag_or,
            description_and: vec![],
            description_or: vec![],
            eng_units_and: vec![],
            eng_units_or: vec![],
            include_properties: false,
        };
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.search_tags(req)).map_err(err)?;
        Ok(resp.search.iter().map(|s| s.tag_name.clone()).collect())
    }

    /// Browse by tree path.
    ///
    /// Returns a list of dicts, one per node on the path.
    fn browse_path(&mut self, py: Python<'_>, tree_path: Vec<String>) -> PyResult<PyObject> {
        let c = self.client.as_mut().ok_or_else(|| err("disconnected"))?;
        let resp = self.rt.block_on(c.browse_path(tree_path)).map_err(err)?;
        let result = PyList::empty(py);
        for node in &resp.nodes {
            let d = PyDict::new(py);
            d.set_item("parent_id_path", &node.parent_id_path)?;
            d.set_item("error_message", &node.error_message)?;
            let children = PyList::empty(py);
            for child in &node.children {
                let cd = PyDict::new(py);
                cd.set_item("id_path", &child.id_path)?;
                cd.set_item("text", &child.text)?;
                cd.set_item("num_children", child.num_children)?;
                cd.set_item("num_tags", child.num_tags)?;
                children.append(cd)?;
            }
            d.set_item("children", children)?;
            result.append(d)?;
        }
        Ok(result.into_any().unbind())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<()> {
        self.disconnect()
    }

    fn __repr__(&self) -> String {
        match &self.client {
            Some(c) => format!("CanaryView(cci={})", c.cci()),
            None => "CanaryView(disconnected)".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// ISO 8601 timestamp parsing (basic)
// ---------------------------------------------------------------------------

fn parse_iso_timestamp(s: &str) -> Result<prost_types::Timestamp, String> {
    // Parse "YYYY-MM-DDThh:mm:ss[.nanos]Z" or "YYYY-MM-DD hh:mm:ss"
    let s = s.trim().trim_end_matches('Z');
    let (date_part, time_part) = if let Some(pos) = s.find('T') {
        (&s[..pos], &s[pos + 1..])
    } else if let Some(pos) = s.find(' ') {
        (&s[..pos], &s[pos + 1..])
    } else {
        (s, "00:00:00")
    };

    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        return Err(format!("invalid date: {}", date_part));
    }
    let year: i64 = date_parts[0].parse().map_err(|_| format!("invalid year: {}", date_parts[0]))?;
    let month: i64 = date_parts[1].parse().map_err(|_| format!("invalid month: {}", date_parts[1]))?;
    let day: i64 = date_parts[2].parse().map_err(|_| format!("invalid day: {}", date_parts[2]))?;

    let (time_whole, nanos) = if let Some(dot_pos) = time_part.find('.') {
        let nano_str = &time_part[dot_pos + 1..];
        let padded = format!("{:0<9}", nano_str);
        let nanos: i32 = padded[..9].parse().map_err(|_| format!("invalid nanos: {}", nano_str))?;
        (&time_part[..dot_pos], nanos)
    } else {
        (time_part, 0)
    };

    let time_parts: Vec<&str> = time_whole.split(':').collect();
    let hours: i64 = time_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    let mins: i64 = time_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let secs: i64 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    // Convert to Unix epoch seconds
    let mut total_days: i64 = 0;
    for y in 1970..year {
        total_days += if is_leap(y) { 366 } else { 365 };
    }
    let leap = is_leap(year);
    let month_days: [i64; 12] = [
        31, if leap { 29 } else { 28 }, 31, 30, 31, 30,
        31, 31, 30, 31, 30, 31,
    ];
    for m in 0..(month - 1) as usize {
        if m < 12 {
            total_days += month_days[m];
        }
    }
    total_days += day - 1;

    let epoch_secs = total_days * 86400 + hours * 3600 + mins * 60 + secs;

    Ok(prost_types::Timestamp {
        seconds: epoch_secs,
        nanos,
    })
}

// ---------------------------------------------------------------------------
// Module definition
// ---------------------------------------------------------------------------

#[pymodule]
pub fn crowsong(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CanaryView>()?;
    Ok(())
}
