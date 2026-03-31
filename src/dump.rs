use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use libpgdump::types::ObjectType;

use crate::entry::PyEntry;
use crate::errors::to_pyerr;

#[pyclass(name = "Dump")]
pub struct PyDump {
    inner: Mutex<libpgdump::Dump>,
}

#[pymethods]
impl PyDump {
    #[new]
    #[pyo3(signature = (dbname="pgdumplib", encoding="UTF8", appear_as="18.0"))]
    fn new(dbname: &str, encoding: &str, appear_as: &str) -> PyResult<Self> {
        let dump = libpgdump::Dump::new(dbname, encoding, appear_as)
            .map_err(to_pyerr)?;
        Ok(PyDump {
            inner: Mutex::new(dump),
        })
    }

    #[staticmethod]
    fn load(path: &str) -> PyResult<Self> {
        let dump = libpgdump::Dump::load(path).map_err(to_pyerr)?;
        Ok(PyDump {
            inner: Mutex::new(dump),
        })
    }

    fn save(&self, path: &str) -> PyResult<()> {
        self.inner.lock().unwrap().save(path).map_err(to_pyerr)
    }

    #[getter]
    fn version(&self) -> (i32, i32, i32) {
        let dump = self.inner.lock().unwrap();
        let v = dump.version();
        (v.major as i32, v.minor as i32, v.rev as i32)
    }

    #[getter]
    fn compression(&self) -> String {
        self.inner.lock().unwrap().compression().to_string()
    }

    #[getter]
    fn dbname(&self) -> String {
        self.inner.lock().unwrap().dbname().to_string()
    }

    #[getter]
    fn server_version(&self) -> String {
        self.inner.lock().unwrap().server_version().to_string()
    }

    #[getter]
    fn dump_version(&self) -> String {
        self.inner.lock().unwrap().dump_version().to_string()
    }

    #[getter]
    fn timestamp<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dump = self.inner.lock().unwrap();
        let ts = dump.timestamp();
        let datetime_mod = py.import("datetime")?;
        let utc = datetime_mod.getattr("timezone")?.getattr("utc")?;
        datetime_mod.getattr("datetime")?.call1((
            ts.year + 1900,
            ts.month + 1,
            ts.day,
            ts.hour,
            ts.minute,
            ts.second,
            0i32,
            &utc,
        ))
    }

    #[getter]
    fn entries(&self) -> Vec<PyEntry> {
        self.inner
            .lock()
            .unwrap()
            .entries()
            .iter()
            .map(PyEntry::from)
            .collect()
    }

    fn entry_count(&self) -> usize {
        self.inner.lock().unwrap().entries().len()
    }

    fn entry_dump_ids(&self) -> Vec<i32> {
        self.inner
            .lock()
            .unwrap()
            .entries()
            .iter()
            .map(|e| e.dump_id)
            .collect()
    }

    fn get_entry(&self, dump_id: i32) -> Option<PyEntry> {
        self.inner
            .lock()
            .unwrap()
            .get_entry(dump_id)
            .map(PyEntry::from)
    }

    fn lookup_entry(
        &self,
        desc: &str,
        namespace: &str,
        tag: &str,
    ) -> Option<PyEntry> {
        let dump = self.inner.lock().unwrap();
        let obj_type: ObjectType = desc.into();
        dump.lookup_entry(&obj_type, namespace, tag)
            .map(PyEntry::from)
    }

    fn table_data(
        &self,
        namespace: &str,
        table: &str,
    ) -> PyResult<Vec<String>> {
        let dump = self.inner.lock().unwrap();
        let iter =
            dump.table_data(namespace, table).map_err(to_pyerr)?;
        Ok(iter.map(String::from).collect())
    }

    fn blobs<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let dump = self.inner.lock().unwrap();
        let blob_list = dump.blobs();
        let mut result = Vec::with_capacity(blob_list.len());
        for (oid, data) in blob_list {
            let py_bytes = PyBytes::new(py, data);
            let tuple = PyTuple::new(
                py,
                &[
                    oid.into_pyobject(py)?.into_any(),
                    py_bytes.into_any(),
                ],
            )?;
            result.push(tuple);
        }
        Ok(result)
    }

    fn entry_data<'py>(
        &self,
        py: Python<'py>,
        dump_id: i32,
    ) -> Option<Bound<'py, PyBytes>> {
        let dump = self.inner.lock().unwrap();
        dump.entry_data(dump_id)
            .map(|data| PyBytes::new(py, data))
    }

    #[pyo3(signature = (
        desc,
        namespace=None,
        tag=None,
        owner=None,
        defn=None,
        drop_stmt=None,
        copy_stmt=None,
        dependencies=None,
    ))]
    fn add_entry(
        &self,
        desc: &str,
        namespace: Option<&str>,
        tag: Option<&str>,
        owner: Option<&str>,
        defn: Option<&str>,
        drop_stmt: Option<&str>,
        copy_stmt: Option<&str>,
        dependencies: Option<Vec<i32>>,
    ) -> PyResult<i32> {
        let deps = dependencies.unwrap_or_default();
        let obj_type: ObjectType = desc.into();
        self.inner
            .lock()
            .unwrap()
            .add_entry(
                obj_type, namespace, tag, owner, defn, drop_stmt,
                copy_stmt, &deps,
            )
            .map_err(to_pyerr)
    }

    fn set_entry_data(
        &self,
        dump_id: i32,
        data: &[u8],
    ) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .set_entry_data(dump_id, data.to_vec())
            .map_err(to_pyerr)
    }

    fn add_blob(&self, oid: i32, data: &[u8]) -> PyResult<i32> {
        self.inner
            .lock()
            .unwrap()
            .add_blob(oid, data.to_vec())
            .map_err(to_pyerr)
    }

    #[pyo3(signature = (
        dump_id,
        tableam=None,
        tablespace=None,
        defn=None,
    ))]
    fn update_entry(
        &self,
        dump_id: i32,
        tableam: Option<&str>,
        tablespace: Option<&str>,
        defn: Option<&str>,
    ) -> PyResult<()> {
        let mut dump = self.inner.lock().unwrap();
        let entry = dump.get_entry_mut(dump_id).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Entry with dump_id {dump_id} not found"
            ))
        })?;
        if let Some(v) = tableam {
            entry.tableam = Some(v.to_string());
        }
        if let Some(v) = tablespace {
            entry.tablespace = Some(v.to_string());
        }
        if let Some(v) = defn {
            entry.defn = Some(v.to_string());
        }
        Ok(())
    }

    fn set_format(&self, fmt: &str) -> PyResult<()> {
        let format = match fmt.to_lowercase().as_str() {
            "custom" => libpgdump::Format::Custom,
            "directory" => libpgdump::Format::Directory,
            "tar" => libpgdump::Format::Tar,
            _ => {
                return Err(
                    pyo3::exceptions::PyValueError::new_err(
                        format!("Unknown format: {fmt}"),
                    ),
                )
            }
        };
        self.inner.lock().unwrap().set_format(format);
        Ok(())
    }

    fn set_compression(&self, alg: &str) -> PyResult<()> {
        let compression = match alg.to_lowercase().as_str() {
            "none" => libpgdump::CompressionAlgorithm::None,
            "gzip" => libpgdump::CompressionAlgorithm::Gzip,
            "lz4" => libpgdump::CompressionAlgorithm::Lz4,
            "zstd" => libpgdump::CompressionAlgorithm::Zstd,
            _ => {
                return Err(
                    pyo3::exceptions::PyValueError::new_err(
                        format!(
                            "Unknown compression algorithm: {alg}"
                        ),
                    ),
                )
            }
        };
        self.inner.lock().unwrap().set_compression(compression);
        Ok(())
    }
}
