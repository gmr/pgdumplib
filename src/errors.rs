use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

fn import_exception(
    class_name: &str,
    args: &[&str],
) -> Option<PyErr> {
    Python::try_attach(|py| {
        let module = py.import("pgdumplib.exceptions")?;
        let cls = module.getattr(class_name)?;
        let py_args: Vec<_> = args
            .iter()
            .map(|a| a.into_pyobject(py).unwrap().into_any())
            .collect();
        let tuple =
            pyo3::types::PyTuple::new(py, &py_args)?;
        let exc = cls.call1(tuple)?;
        PyResult::Ok(PyErr::from_value(exc.into_any()))
    })?
    .ok()
}

pub fn to_pyerr(err: libpgdump::Error) -> PyErr {
    match err {
        libpgdump::Error::Io(e) => PyIOError::new_err(e.to_string()),
        libpgdump::Error::EntityNotFound {
            namespace, tag, ..
        } => {
            import_exception(
                "EntityNotFoundError",
                &[namespace.as_str(), tag.as_str()],
            )
            .unwrap_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "Entity not found: {namespace}.{tag}"
                ))
            })
        }
        libpgdump::Error::NoData(_) => {
            import_exception("NoDataError", &[])
                .unwrap_or_else(|| {
                    PyRuntimeError::new_err("No data entries exist")
                })
        }
        libpgdump::Error::InvalidHeader(msg) => {
            PyValueError::new_err(msg)
        }
        libpgdump::Error::UnsupportedVersion(v) => {
            PyValueError::new_err(format!(
                "Unsupported backup version: {v}"
            ))
        }
        libpgdump::Error::UnsupportedFormat(f) => {
            PyValueError::new_err(format!(
                "Unsupported format: {f}"
            ))
        }
        libpgdump::Error::UnsupportedCompression(c) => {
            PyValueError::new_err(format!(
                "Unsupported compression algorithm: {c}"
            ))
        }
        libpgdump::Error::InvalidDumpId(id) => {
            PyValueError::new_err(format!(
                "Invalid dump ID: {id}"
            ))
        }
        libpgdump::Error::DataIntegrity(msg) => {
            PyRuntimeError::new_err(msg)
        }
        libpgdump::Error::Decompression(msg) => {
            PyRuntimeError::new_err(msg)
        }
        libpgdump::Error::InvalidUtf8(e) => {
            PyValueError::new_err(e.to_string())
        }
    }
}
