mod dump;
mod entry;
mod errors;

use pyo3::prelude::*;

#[pymodule]
fn _pgdumplib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<dump::PyDump>()?;
    m.add_class::<entry::PyEntry>()?;
    Ok(())
}
