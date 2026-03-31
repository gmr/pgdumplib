use pyo3::prelude::*;

#[pyclass(name = "Entry", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyEntry {
    #[pyo3(get)]
    pub dump_id: i32,
    #[pyo3(get)]
    pub had_dumper: bool,
    #[pyo3(get)]
    pub table_oid: String,
    #[pyo3(get)]
    pub oid: String,
    #[pyo3(get)]
    pub tag: Option<String>,
    #[pyo3(get)]
    pub desc: String,
    #[pyo3(get)]
    pub section: String,
    #[pyo3(get)]
    pub defn: Option<String>,
    #[pyo3(get)]
    pub drop_stmt: Option<String>,
    #[pyo3(get)]
    pub copy_stmt: Option<String>,
    #[pyo3(get)]
    pub namespace: Option<String>,
    #[pyo3(get)]
    pub tablespace: Option<String>,
    #[pyo3(get)]
    pub tableam: Option<String>,
    #[pyo3(get)]
    pub relkind: Option<String>,
    #[pyo3(get)]
    pub owner: Option<String>,
    #[pyo3(get)]
    pub with_oids: bool,
    #[pyo3(get)]
    pub dependencies: Vec<i32>,
    #[pyo3(get)]
    pub data_state: i32,
    #[pyo3(get)]
    pub offset: u64,
}

#[pymethods]
impl PyEntry {
    fn __repr__(&self) -> String {
        format!(
            "<Entry dump_id={} desc={:?} tag={:?} namespace={:?}>",
            self.dump_id, self.desc, self.tag, self.namespace
        )
    }
}

impl From<&libpgdump::Entry> for PyEntry {
    fn from(entry: &libpgdump::Entry) -> Self {
        PyEntry {
            dump_id: entry.dump_id,
            had_dumper: entry.had_dumper,
            table_oid: entry.table_oid.clone(),
            oid: entry.oid.clone(),
            tag: entry.tag.clone(),
            desc: entry.desc.to_string(),
            section: entry.section.to_string(),
            defn: entry.defn.clone(),
            drop_stmt: entry.drop_stmt.clone(),
            copy_stmt: entry.copy_stmt.clone(),
            namespace: entry.namespace.clone(),
            tablespace: entry.tablespace.clone(),
            tableam: entry.tableam.clone(),
            relkind: entry.relkind.map(|c| c.to_string()),
            owner: entry.owner.clone(),
            with_oids: entry.with_oids,
            dependencies: entry.dependencies.clone(),
            data_state: entry.data_state as i32,
            offset: entry.offset,
        }
    }
}
