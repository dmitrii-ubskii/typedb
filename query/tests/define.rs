/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use query::query_manager::QueryManager;
use storage::snapshot::CommittableSnapshot;

use crate::common::{load_managers, setup_storage};

mod common;

#[test]
fn basic() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager, _) = load_managers(storage.clone());
    let mut snapshot = storage.clone().open_snapshot_schema();
    let query_manager = QueryManager::new();

    let query_str = r#"
    define
    attribute name value string;
    entity person owns name;
    "#;
    let schema_query = typeql::parse_query(query_str).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, schema_query).unwrap();
    snapshot.commit().unwrap();
}
