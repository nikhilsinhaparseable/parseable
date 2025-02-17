/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

 pub mod catalog;
 mod filter_optimizer;
 pub mod functions;
 mod listing_table_builder;
 pub mod object_storage;
 pub mod stream_schema_provider;
 
 use arrow_schema::SortOptions;
 // use catalog::DynamicObjectStoreCatalog;
 use chrono::NaiveDateTime;
 use chrono::{DateTime, Duration, Utc};
 use datafusion::arrow::record_batch::RecordBatch;
 
 use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
 use datafusion::common::DFSchema;
 use datafusion::config::Extensions;
// use datafusion::common::{exec_datafusion_err, plan_err};
 // use datafusion::config::ConfigFileType;
 use datafusion::error::{DataFusionError, Result};
 use datafusion::execution::disk_manager::DiskManagerConfig;
 use datafusion::execution::runtime_env::RuntimeEnvBuilder;
 // use datafusion::execution::runtime_env::RuntimeEnvBuilder;
 use datafusion::execution::{SessionState, SessionStateBuilder};
 use datafusion::logical_expr::expr::Alias;
 use datafusion::logical_expr::{
     Aggregate, Explain, Filter, LogicalPlan, PlanType, Projection, ToStringifiedPlan,
 };
 use datafusion::physical_expr::{create_physical_expr, LexOrdering, PhysicalSortExpr};
 use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
 use datafusion::physical_plan::filter::FilterExec;
 use datafusion::physical_plan::repartition::RepartitionExec;
 use datafusion::physical_plan::sorts::sort::SortExec;
 use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
 use datafusion::physical_plan::{collect, ExecutionPlan, Partitioning};
 // use datafusion::physical_plan::execution_plan::EmissionType;
 // use datafusion::physical_plan::{collect, execute_stream, ExecutionPlanProperties};
 use datafusion::prelude::*;
 // use datafusion::sql::parser::DFParser;
 use itertools::Itertools;
 use once_cell::sync::Lazy;
 use relative_path::RelativePathBuf;
 use serde::{Deserialize, Serialize};
 use serde_json::{json, Value};
 use std::ops::Bound;
 // use std::path::{Path, PathBuf};
 use std::sync::Arc;
 use std::time::Instant;
 use stream_schema_provider::collect_manifest_files;
 use sysinfo::System;
 
 use std::fs;
 
 use self::error::ExecuteError;
 use self::stream_schema_provider::GlobalSchemaProvider;
 pub use self::stream_schema_provider::PartialTimeFilter;
 use crate::catalog::column::{Int64Type, TypedStatistics};
 use crate::catalog::manifest::Manifest;
 use crate::catalog::snapshot::Snapshot;
 use crate::catalog::Snapshot as CatalogSnapshot;
 use crate::event;
 use crate::handlers::http::query::QueryError;
 use crate::metadata::STREAM_INFO;
 use crate::option::{Mode, CONFIG};
 use crate::storage::{ObjectStorageProvider, ObjectStoreFormat, STREAM_ROOT_DIRECTORY};
 use crate::utils::time::TimeRange;
 pub static QUERY_SESSION: Lazy<SessionContext> =
     Lazy::new(|| Query::create_session_context(CONFIG.storage()));
 
 // A query request by client
 #[derive(Debug)]
 pub struct Query {
     pub raw_logical_plan: LogicalPlan,
     pub time_range: TimeRange,
     pub filter_tag: Option<Vec<String>>,
 }
 
 impl Query {
     // create session context for this query
     pub fn create_session_context(storage: Arc<dyn ObjectStorageProvider>) -> SessionContext {
         let runtime_config = storage
             .get_datafusion_runtime()
             .with_disk_manager(DiskManagerConfig::NewOs);
 
         let (pool_size, fraction) = match CONFIG.options.query_memory_pool_size {
             Some(size) => (size, 1.),
             None => {
                 let mut system = System::new();
                 system.refresh_memory();
                 let available_mem = system.available_memory();
                 (available_mem as usize, 0.85)
             }
         };
 
         let runtime_config = runtime_config.with_memory_limit(pool_size, fraction);
         let runtime = Arc::new(runtime_config.build().unwrap());
         
         let mut config = SessionConfig::default()
             .with_parquet_pruning(true)
             //.with_prefer_existing_sort(true)
             .with_round_robin_repartition(true);
 
         // For more details refer https://datafusion.apache.org/user-guide/configs.html
 
         // Reduce the number of rows read (if possible)
         //config.options_mut().execution.parquet.enable_page_index = true;
 
         // Pushdown filters allows DF to push the filters as far down in the plan as possible
         // and thus, reducing the number of rows decoded
         config.options_mut().execution.parquet.pushdown_filters = true;
 
         // Reorder filters allows DF to decide the order of filters minimizing the cost of filter evaluation
         // config.options_mut().execution.parquet.reorder_filters = true;
 
         // Enable StringViewArray
         // https://www.influxdata.com/blog/faster-queries-with-stringview-part-one-influxdb/
         // config
         //     .options_mut()
         //     .execution
         //     .parquet
         //     .schema_force_view_types = true;
         config.options_mut().execution.parquet.binary_as_string = true;
 
         let state = SessionStateBuilder::new()
             .with_default_features()
             .with_config(config)
             .with_runtime_env(runtime)
             .build();
 
         let schema_provider = Arc::new(GlobalSchemaProvider {
             storage: storage.get_object_store(),
         });
         state
             .catalog_list()
             .catalog(&state.config_options().catalog.default_catalog)
             .expect("default catalog is provided by datafusion")
             .register_schema(
                 &state.config_options().catalog.default_schema,
                 schema_provider,
             )
             .unwrap();
 
         SessionContext::new_with_state(state)
     }
 
     pub async fn execute(
         &self,
         stream_name: String,
     ) -> Result<(Vec<RecordBatch>, Vec<String>), ExecuteError> {
         let time_partition = STREAM_INFO.get_time_partition(&stream_name)?;
 
         let df = QUERY_SESSION
             .execute_logical_plan(self.final_logical_plan(&time_partition))
             .await?;
 
         let fields = df
             .schema()
             .fields()
             .iter()
             .map(|f| f.name())
             .cloned()
             .collect_vec();
 
         if fields.is_empty() {
             return Ok((vec![], fields));
         }
 
         let results = df.collect().await?;
         Ok((results, fields))
     }
 
     pub async fn get_dataframe(&self, stream_name: String) -> Result<DataFrame, ExecuteError> {
         let time_partition = STREAM_INFO.get_time_partition(&stream_name)?;
 
         let df = QUERY_SESSION
             .execute_logical_plan(self.final_logical_plan(&time_partition))
             .await?;
 
         Ok(df)
     }
 
     /// return logical plan with all time filters applied through
     fn final_logical_plan(&self, time_partition: &Option<String>) -> LogicalPlan {
         // see https://github.com/apache/arrow-datafusion/pull/8400
         // this can be eliminated in later version of datafusion but with slight caveat
         // transform cannot modify stringified plans by itself
         // we by knowing this plan is not in the optimization procees chose to overwrite the stringified plan
 
         match self.raw_logical_plan.clone() {
             LogicalPlan::Explain(plan) => {
                 let transformed = transform(
                     plan.plan.as_ref().clone(),
                     self.time_range.start.naive_utc(),
                     self.time_range.end.naive_utc(),
                     time_partition,
                 );
                 LogicalPlan::Explain(Explain {
                     verbose: plan.verbose,
                     stringified_plans: vec![transformed
                         .data
                         .to_stringified(PlanType::InitialLogicalPlan)],
                     plan: Arc::new(transformed.data),
                     schema: plan.schema,
                     logical_optimization_succeeded: plan.logical_optimization_succeeded,
                 })
             }
             x => {
                 transform(
                     x,
                     self.time_range.start.naive_utc(),
                     self.time_range.end.naive_utc(),
                     time_partition,
                 )
                 .data
             }
         }
     }
 
     pub fn first_table_name(&self) -> Option<String> {
         let mut visitor = TableScanVisitor::default();
         let _ = self.raw_logical_plan.visit(&mut visitor);
         visitor.into_inner().pop()
     }
 
     /// Evaluates to Some("count(*)") | Some("column_name") if the logical plan is a Projection: SELECT COUNT(*) | SELECT COUNT(*) as column_name
     pub fn is_logical_plan_count_without_filters(&self) -> Option<&String> {
         // Check if the raw logical plan is a Projection: SELECT
         let LogicalPlan::Projection(Projection { input, expr, .. }) = &self.raw_logical_plan else {
             return None;
         };
         // Check if the input of the Projection is an Aggregate: COUNT(*)
         let LogicalPlan::Aggregate(Aggregate { input, .. }) = &**input else {
             return None;
         };
 
         // Ensure the input of the Aggregate is a TableScan and there is exactly one expression: SELECT COUNT(*)
         if !matches!(&**input, LogicalPlan::TableScan { .. }) || expr.len() != 1 {
             return None;
         }
 
         // Check if the expression is a column or an alias for COUNT(*)
         match &expr[0] {
             // Direct column check
             Expr::Column(Column { name, .. }) if name.to_lowercase() == "count(*)" => Some(name),
             // Alias for COUNT(*)
             Expr::Alias(Alias {
                 expr: inner_expr,
                 name: alias_name,
                 ..
             }) => {
                 if let Expr::Column(Column { name, .. }) = &**inner_expr {
                     if name.to_lowercase() == "count(*)" {
                         return Some(alias_name);
                     }
                 }
                 None
             }
             // Unsupported expression type
             _ => None,
         }
     }
 }
 
 /// Record of counts for a given time bin.
 #[derive(Debug, Serialize, Clone)]
 pub struct CountsRecord {
     /// Start time of the bin
     pub start_time: String,
     /// End time of the bin
     pub end_time: String,
     /// Number of logs in the bin
     pub count: u64,
 }
 
 struct TimeBounds {
     start: DateTime<Utc>,
     end: DateTime<Utc>,
 }
 
 /// Request for counts, received from API/SQL query.
 #[derive(Debug, Deserialize, Clone)]
 #[serde(rename_all = "camelCase")]
 pub struct CountsRequest {
     /// Name of the stream to get counts for
     pub stream: String,
     /// Included start time for counts query
     pub start_time: String,
     /// Excluded end time for counts query
     pub end_time: String,
     /// Number of bins to divide the time range into
     pub num_bins: u64,
 }
 
 impl CountsRequest {
     /// This function is supposed to read maninfest files for the given stream,
     /// get the sum of `num_rows` between the `startTime` and `endTime`,
     /// divide that by number of bins and return in a manner acceptable for the console
     pub async fn get_bin_density(&self) -> Result<Vec<CountsRecord>, QueryError> {
         let time_partition = STREAM_INFO
             .get_time_partition(&self.stream.clone())
             .map_err(|err| anyhow::Error::msg(err.to_string()))?
             .unwrap_or_else(|| event::DEFAULT_TIMESTAMP_KEY.to_owned());
 
         // get time range
         let time_range = TimeRange::parse_human_time(&self.start_time, &self.end_time)?;
         let all_manifest_files = get_manifest_list(&self.stream, &time_range).await?;
         // get bounds
         let counts = self.get_bounds(&time_range);
 
         // we have start and end times for each bin
         // we also have all the manifest files for the given time range
         // now we iterate over start and end times for each bin
         // then we iterate over the manifest files which are within that time range
         // we sum up the num_rows
         let mut counts_records = Vec::new();
 
         for bin in counts {
             // extract start and end time to compare
             // Sum up the number of rows that fall within the bin
             let count: u64 = all_manifest_files
                 .iter()
                 .flat_map(|m| &m.files)
                 .filter_map(|f| {
                     if f.columns.iter().any(|c| {
                         c.name == time_partition
                             && c.stats.as_ref().is_some_and(|stats| match stats {
                                 TypedStatistics::Int(Int64Type { min, .. }) => {
                                     let min = DateTime::from_timestamp_millis(*min).unwrap();
                                     bin.start <= min && bin.end >= min // Determines if a column matches the bin's time range.
                                 }
                                 _ => false,
                             })
                     }) {
                         Some(f.num_rows)
                     } else {
                         None
                     }
                 })
                 .sum();
 
             counts_records.push(CountsRecord {
                 start_time: bin.start.to_rfc3339(),
                 end_time: bin.end.to_rfc3339(),
                 count,
             });
         }
         Ok(counts_records)
     }
 
     /// Calculate the end time for each bin based on the number of bins
     fn get_bounds(&self, time_range: &TimeRange) -> Vec<TimeBounds> {
         let total_minutes = time_range
             .end
             .signed_duration_since(time_range.start)
             .num_minutes() as u64;
 
         // divide minutes by num bins to get minutes per bin
         let quotient = total_minutes / self.num_bins;
         let remainder = total_minutes % self.num_bins;
         let have_remainder = remainder > 0;
 
         // now create multiple bounds [startTime, endTime)
         // Should we exclude the last one???
         let mut bounds = vec![];
 
         let mut start = time_range.start;
 
         let loop_end = if have_remainder {
             self.num_bins
         } else {
             self.num_bins - 1
         };
 
         // Create bins for all but the last date
         for _ in 0..loop_end {
             let end = start + Duration::minutes(quotient as i64);
             bounds.push(TimeBounds { start, end });
             start = end;
         }
 
         // Add the last bin, accounting for any remainder, should we include it?
         if have_remainder {
             bounds.push(TimeBounds {
                 start,
                 end: start + Duration::minutes(remainder as i64),
             });
         } else {
             bounds.push(TimeBounds {
                 start,
                 end: start + Duration::minutes(quotient as i64),
             });
         }
 
         bounds
     }
 }
 
 /// Response for the counts API
 #[derive(Debug, Serialize, Clone)]
 pub struct CountsResponse {
     /// Fields in the log stream
     pub fields: Vec<String>,
     /// Records in the response
     pub records: Vec<CountsRecord>,
 }
 
 #[derive(Debug, Default)]
 pub struct TableScanVisitor {
     tables: Vec<String>,
 }
 
 impl TableScanVisitor {
     pub fn into_inner(self) -> Vec<String> {
         self.tables
     }
 }
 
 impl TreeNodeVisitor<'_> for TableScanVisitor {
     type Node = LogicalPlan;
 
     fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
         match node {
             LogicalPlan::TableScan(table) => {
                 self.tables.push(table.table_name.table().to_string());
                 Ok(TreeNodeRecursion::Jump)
             }
             _ => Ok(TreeNodeRecursion::Continue),
         }
     }
 }
 
 pub async fn get_manifest_list(
     stream_name: &str,
     time_range: &TimeRange,
 ) -> Result<Vec<Manifest>, QueryError> {
     let glob_storage = CONFIG.storage().get_object_store();
 
     let object_store = QUERY_SESSION
         .state()
         .runtime_env()
         .object_store_registry
         .get_store(&glob_storage.store_url())
         .unwrap();
 
     // get object store
     let object_store_format = glob_storage
         .get_object_store_format(stream_name)
         .await
         .map_err(|err| DataFusionError::Plan(err.to_string()))?;
 
     // all the manifests will go here
     let mut merged_snapshot: Snapshot = Snapshot::default();
 
     // get a list of manifests
     if CONFIG.options.mode == Mode::Query {
         let path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
         let obs = glob_storage
             .get_objects(
                 Some(&path),
                 Box::new(|file_name| file_name.ends_with("stream.json")),
             )
             .await;
         if let Ok(obs) = obs {
             for ob in obs {
                 if let Ok(object_store_format) = serde_json::from_slice::<ObjectStoreFormat>(&ob) {
                     let snapshot = object_store_format.snapshot;
                     for manifest in snapshot.manifest_list {
                         merged_snapshot.manifest_list.push(manifest);
                     }
                 }
             }
         }
     } else {
         merged_snapshot = object_store_format.snapshot;
     }
 
     // Download all the manifest files
     let time_filter = [
         PartialTimeFilter::Low(Bound::Included(time_range.start.naive_utc())),
         PartialTimeFilter::High(Bound::Included(time_range.end.naive_utc())),
     ];
 
     let all_manifest_files = collect_manifest_files(
         object_store,
         merged_snapshot
             .manifests(&time_filter)
             .into_iter()
             .sorted_by_key(|file| file.time_lower_bound)
             .map(|item| item.manifest_path)
             .collect(),
     )
     .await
     .map_err(|err| anyhow::Error::msg(err.to_string()))?;
 
     Ok(all_manifest_files)
 }
 
 fn transform(
     plan: LogicalPlan,
     start_time: NaiveDateTime,
     end_time: NaiveDateTime,
     time_partition: &Option<String>,
 ) -> Transformed<LogicalPlan> {
     plan.transform(&|plan| match plan {
         LogicalPlan::TableScan(table) => {
             let new_filters = vec![];
             if !table_contains_any_time_filters(&table, time_partition) {
                 let mut _start_time_filter: Expr;
                 let mut _end_time_filter: Expr;
                 match time_partition {
                     Some(time_partition) => {
                         _start_time_filter =
                             PartialTimeFilter::Low(std::ops::Bound::Included(start_time))
                                 .binary_expr(Expr::Column(Column::new(
                                     Some(table.table_name.to_owned()),
                                     time_partition.clone(),
                                 )));
                         _end_time_filter =
                             PartialTimeFilter::High(std::ops::Bound::Excluded(end_time))
                                 .binary_expr(Expr::Column(Column::new(
                                     Some(table.table_name.to_owned()),
                                     time_partition,
                                 )));
                     }
                     None => {
                         _start_time_filter =
                             PartialTimeFilter::Low(std::ops::Bound::Included(start_time))
                                 .binary_expr(Expr::Column(Column::new(
                                     Some(table.table_name.to_owned()),
                                     event::DEFAULT_TIMESTAMP_KEY,
                                 )));
                         _end_time_filter =
                             PartialTimeFilter::High(std::ops::Bound::Excluded(end_time))
                                 .binary_expr(Expr::Column(Column::new(
                                     Some(table.table_name.to_owned()),
                                     event::DEFAULT_TIMESTAMP_KEY,
                                 )));
                     }
                 }
 
                 //new_filters.push(_start_time_filter);
                 //new_filters.push(_end_time_filter);
             }
             let new_filter = new_filters.into_iter().reduce(and);
             if let Some(new_filter) = new_filter {
                 let filter =
                     Filter::try_new(new_filter, Arc::new(LogicalPlan::TableScan(table))).unwrap();
                 Ok(Transformed::yes(LogicalPlan::Filter(filter)))
             } else {
                 Ok(Transformed::no(LogicalPlan::TableScan(table)))
             }
         }
         x => Ok(Transformed::no(x)),
     })
     .expect("transform only transforms the tablescan")
 }
 
 fn table_contains_any_time_filters(
     table: &datafusion::logical_expr::TableScan,
     time_partition: &Option<String>,
 ) -> bool {
     table
         .filters
         .iter()
         .filter_map(|x| {
             if let Expr::BinaryExpr(binexpr) = x {
                 Some(binexpr)
             } else {
                 None
             }
         })
         .any(|expr| {
             matches!(&*expr.left, Expr::Column(Column { name, .. })
              if ((time_partition.is_some() && name == time_partition.as_ref().unwrap()) ||
              (!time_partition.is_some() && name == event::DEFAULT_TIMESTAMP_KEY)))
         })
 }
 
 /// unused for now might need it later
 #[allow(unused)]
 pub fn flatten_objects_for_count(objects: Vec<Value>) -> Vec<Value> {
     if objects.is_empty() {
         return objects;
     }
 
     // check if all the keys start with "COUNT"
     let flag = objects.iter().all(|obj| {
         obj.as_object()
             .unwrap()
             .keys()
             .all(|key| key.starts_with("COUNT"))
     }) && objects.iter().all(|obj| {
         obj.as_object()
             .unwrap()
             .keys()
             .all(|key| key == objects[0].as_object().unwrap().keys().next().unwrap())
     });
 
     if flag {
         let mut accum = 0u64;
         let key = objects[0]
             .as_object()
             .unwrap()
             .keys()
             .next()
             .unwrap()
             .clone();
 
         for obj in objects {
             let count = obj.as_object().unwrap().keys().fold(0, |acc, key| {
                 let value = obj.as_object().unwrap().get(key).unwrap().as_u64().unwrap();
                 acc + value
             });
             accum += count;
         }
 
         vec![json!({
             key: accum
         })]
     } else {
         objects
     }
 }
 
 // struct AllQueries {
 //     queries: Vec<String>,
 // }
 
 // impl AllQueries {
 //     fn try_new(path: &Path) -> Result<Self> {
 //         // ClickBench has all queries in a single file identified by line number
 //         let all_queries = std::fs::read_to_string(path)
 //             .map_err(|e| exec_datafusion_err!("Could not open {path:?}: {e}"))?;
 //         Ok(Self {
 //             queries: all_queries.lines().map(|s| s.to_string()).collect(),
 //         })
 //     }
 
 //     /// Returns the text of query `query_id`
 //     fn get_query(&self, query_id: usize) -> Result<&str> {
 //         self.queries
 //             .get(query_id)
 //             .ok_or_else(|| {
 //                 let min_id = self.min_query_id();
 //                 let max_id = self.max_query_id();
 //                 exec_datafusion_err!(
 //                     "Invalid query id {query_id}. Must be between {min_id} and {max_id}"
 //                 )
 //             })
 //             .map(|s| s.as_str())
 //     }
 
 //     fn min_query_id(&self) -> usize {
 //         0
 //     }
 
 //     fn max_query_id(&self) -> usize {
 //         self.queries.len() - 1
 //     }
 // }
 
 // pub async fn run() -> Result<()> {
 //     let rt_config = RuntimeEnvBuilder::new();
 //     let runtime_env = rt_config.build().unwrap();
 //     println!("Running benchmarks");
 //     let queries_path: PathBuf = ["/home", "ubuntu", "queries.sql"]
 //         .iter()
 //         .collect();
 //     let queries = AllQueries::try_new(queries_path.as_path())?;
 //     println!("queries loaded");
 //     let query_range = queries.min_query_id()..=queries.max_query_id();
 
 //     // configure parquet options
 //     let mut config = SessionConfig::new()
 //         .with_parquet_pruning(true)
 //         .with_target_partitions(num_cpus::get())
 //         .with_coalesce_batches(true)
 //         .with_collect_statistics(true)
 //         .with_parquet_page_index_pruning(true);
 //     config.options_mut().execution.parquet.binary_as_string = true;
 //     config.options_mut().execution.parquet.pushdown_filters = true;
 //     config.options_mut().execution.parquet.reorder_filters = true;
 //     config
 //         .options_mut()
 //         .execution
 //         .use_row_number_estimates_to_optimize_partitioning = true;
 //     // enable dynamic file query
 //     let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env)).enable_url_table();
 //     ctx.refresh_catalogs().await?;
 //     // install dynamic catalog provider that can register required object stores
 //     ctx.register_catalog_list(Arc::new(DynamicObjectStoreCatalog::new(
 //         ctx.state().catalog_list().clone(),
 //         ctx.state_weak_ref(),
 //     )));
 
 //     let sql = "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '/home/ubuntu/clickbench/hits.parquet' OPTIONS ('binary_as_string' 'true')";
 //     let task_ctx = ctx.task_ctx();
 //     let dialect = &task_ctx.session_config().options().sql_parser.dialect;
 //     let dialect = sqlparser::dialect::dialect_from_str(dialect).unwrap();
 //     let plan = ctx.state().create_logical_plan(sql).await?;
 //     if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
 //         let format = Some(ConfigFileType::PARQUET);
 //         // Clone and modify the default table options based on the provided options
 //         let mut table_options = ctx.state().default_table_options();
 //         if let Some(format) = format {
 //             table_options.set_config_format(format);
 //         }
 //         table_options.alter_with_string_hash_map(&cmd.options)?;
 
 //         ctx.sql(&sql).await?;
 //         for query_id in query_range {
 //             let sql = queries.get_query(query_id)?;
 //             let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
 //             for statement in statements {
 //                 let plan = ctx.state().statement_to_plan(statement).await?;
 //                 let df = ctx.execute_logical_plan(plan).await?;
 //                 let physical_plan = df.create_physical_plan().await?;
 //                 if physical_plan.boundedness().is_unbounded() {
 //                     if physical_plan.pipeline_behavior() == EmissionType::Final {
 //                         return plan_err!(
 //                             "The given query can generate a valid result only once \
 //                             the source finishes, but the source is unbounded"
 //                         );
 //                     }
 //                     // As the input stream comes, we can generate results.
 //                     // However, memory safety is not guaranteed.
 //                     let start = Instant::now();
 //                     let _ = execute_stream(physical_plan, task_ctx.clone())?;
 //                     let elapsed = start.elapsed().as_secs_f64();
 //                     println!("Query{query_id}  took {elapsed} seconds");
 //                 } else {
 //                     // Bounded stream; collected results are printed after all input consumed.
 //                     let start = Instant::now();
 //                     let _ = collect(physical_plan, task_ctx.clone()).await?;
 //                     let elapsed = start.elapsed().as_secs_f64();
 //                     println!("Q{query_id}  took {elapsed} seconds");
 //                 }
 //             }
 //         }
 //     } else {
 //         return plan_err!("LogicalPlan is not a CreateExternalTable");
 //     }
 
 //     Ok(())
 // }
 
 pub async fn run_benchmark() {
    const TRIES: usize = 1;
    let mut query_num = 1;
    let runtime_config = RuntimeEnvBuilder::new()  // Number of partitions for parallel processing
    .with_disk_manager(DiskManagerConfig::NewOs);


    let runtime = runtime_config.build().unwrap();


    // Create session context
    let mut config = SessionConfig::new().with_coalesce_batches(true)
    .with_collect_statistics(true)
    .with_parquet_bloom_filter_pruning(true)
    .with_parquet_page_index_pruning(true)
    .with_parquet_pruning(true)
    .with_prefer_existing_sort(true)
    .with_repartition_file_scans(true)
    .with_round_robin_repartition(true)
    .with_repartition_sorts(true)
    .with_batch_size(1000000)
    .with_target_partitions(1);
    config.options_mut().execution.parquet.binary_as_string = true;
    config.options_mut().execution.use_row_number_estimates_to_optimize_partitioning = true;
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    config.options_mut().execution.parquet.pruning = true;
    config.options_mut().execution.parquet.reorder_filters = true;
    config.options_mut().optimizer.enable_topk_aggregation = true;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let sql = "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '/home/ubuntu/clickbench/hits.parquet' OPTIONS ('binary_as_string' 'true')";
    let _ = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    // Read queries from file
    let queries = fs::read_to_string("/home/ubuntu/queries.sql").unwrap();
    
    let mut total_elapsed = 0.0;
    for query in queries.lines() {
        fs::write("/tmp/query.sql", &query).unwrap();
    
        for iteration in 1..=TRIES {            
            
            // Create the query plan
            let df = ctx.sql(&query).await.unwrap();
            //let logical_plan = df.logical_plan().clone();
            let physical_plan = df.create_physical_plan().await.unwrap();
    
            // Add coalesce
            let exec_plan: Arc<dyn ExecutionPlan> = Arc::new(CoalesceBatchesExec::new(physical_plan, 1000000));
            let task_ctx = ctx.task_ctx();
            let repartitioned = Arc::new(RepartitionExec::try_new(
                exec_plan,
                Partitioning::RoundRobinBatch(1),
            ).unwrap());
    let start = Instant::now();
            let _query_response = collect(repartitioned, task_ctx).await.unwrap();
    
            let elapsed = start.elapsed().as_secs_f64();
            total_elapsed += elapsed;
            println!("Query {query_num} iteration {iteration} took {elapsed} seconds");
            
        }
        query_num += 1;
    }
    println!("Total time: {total_elapsed} seconds");

}
 fn create_sort_plan(
     logical_plan: &LogicalPlan,
     exec_plan: Arc<dyn ExecutionPlan>,
     schema: &DFSchema,
     state: &SessionState,
 ) -> Result<Arc<dyn ExecutionPlan>> {
     // Extract sort expressions from the logical plan
     let sort_exprs = match logical_plan {
         LogicalPlan::Sort(sort) => {
             // Get sort expressions from Sort node
             sort.expr.clone()
         }
         _ => {
             // No sorting specified in query, return original plan
             return Ok(exec_plan);
         }
     };
 
     // Convert logical sort expressions to physical sort expressions
     let mut physical_sort_exprs = Vec::with_capacity(sort_exprs.len());
     
     for sort_expr in sort_exprs {
         let physical_expr = create_physical_expr(
             &sort_expr.expr,
             schema,
             state.execution_props(),
         )?;
         
         physical_sort_exprs.push(PhysicalSortExpr {
             expr: physical_expr,
             options: SortOptions::new(false, false)
         });
     }
 
 
 
     // Create sort execution plan if we have sort expressions
     if !physical_sort_exprs.is_empty() {
         let ordering = LexOrdering::new(physical_sort_exprs);
         let sort_preserving_merge_plan = Arc::new(SortPreservingMergeExec::new(ordering.clone(), exec_plan).with_fetch(Some(10)));
         Ok(Arc::new(SortExec::new(ordering, sort_preserving_merge_plan).with_preserve_partitioning(true).with_fetch(Some(10))))
     } else {
         Ok(exec_plan)
     }
 }
 
 fn create_filter_plan(
     logical_plan: &LogicalPlan,
     exec_plan: Arc<dyn ExecutionPlan>,
     state: &SessionState,
 ) -> Result<Arc<dyn ExecutionPlan>> {
     // Extract sort expressions from the logical plan
     match logical_plan {
         LogicalPlan::Sort(sort) => {
             // Get sort expressions from Sort node
             create_filter_plan(&sort.input, exec_plan, state)
         }
         LogicalPlan::Filter(filter) => {
             let schema = exec_plan.schema();
             let expr = filter.predicate.clone();
             let df_schema = DFSchema::try_from(Arc::new(schema.as_ref().clone())).unwrap();
             let physical_expr = create_physical_expr(&expr, &df_schema, state.execution_props()).unwrap();
             let filter_exec = FilterExec::try_new(physical_expr, exec_plan).unwrap();
             return Ok(Arc::new(filter_exec));
         }
         LogicalPlan::Limit(limit) => {
             create_filter_plan(&limit.input, exec_plan, state)
         }
         LogicalPlan::Projection(proj) => {
             create_filter_plan(&proj.input, exec_plan, state)
         }
         LogicalPlan::Aggregate(agg) => {
             create_filter_plan(&agg.input, exec_plan, state)
         }
         
         _ => {
             // No sorting specified in query, return original plan
             return Ok(exec_plan);
         }
     }
 
     
 }
 
 pub mod error {
     use crate::{metadata::error::stream_info::MetadataError, storage::ObjectStorageError};
     use datafusion::error::DataFusionError;
 
     #[derive(Debug, thiserror::Error)]
     pub enum ExecuteError {
         #[error("Query Execution failed due to error in object storage: {0}")]
         ObjectStorage(#[from] ObjectStorageError),
         #[error("Query Execution failed due to error in datafusion: {0}")]
         Datafusion(#[from] DataFusionError),
         #[error("Query Execution failed due to error in fetching metadata: {0}")]
         Metadata(#[from] MetadataError),
     }
 }
 
 #[cfg(test)]
 mod tests {
     use serde_json::json;
 
     use crate::query::flatten_objects_for_count;
 
     #[test]
     fn test_flat_simple() {
         let val = vec![
             json!({
                 "COUNT(*)": 1
             }),
             json!({
                 "COUNT(*)": 2
             }),
             json!({
                 "COUNT(*)": 3
             }),
         ];
 
         let out = flatten_objects_for_count(val);
         assert_eq!(out, vec![json!({"COUNT(*)": 6})]);
     }
 
     #[test]
     fn test_flat_empty() {
         let val = vec![];
         let out = flatten_objects_for_count(val.clone());
         assert_eq!(val, out);
     }
 
     #[test]
     fn test_flat_same_multi() {
         let val = vec![json!({"COUNT(ALPHA)": 1}), json!({"COUNT(ALPHA)": 2})];
         let out = flatten_objects_for_count(val.clone());
         assert_eq!(vec![json!({"COUNT(ALPHA)": 3})], out);
     }
 
     #[test]
     fn test_flat_diff_multi() {
         let val = vec![json!({"COUNT(ALPHA)": 1}), json!({"COUNT(BETA)": 2})];
         let out = flatten_objects_for_count(val.clone());
         assert_eq!(out, val);
     }
 
     #[test]
     fn test_flat_fail() {
         let val = vec![
             json!({
                 "Num": 1
             }),
             json!({
                 "Num": 2
             }),
             json!({
                 "Num": 3
             }),
         ];
 
         let out = flatten_objects_for_count(val.clone());
         assert_eq!(val, out);
     }
 
     #[test]
     fn test_flat_multi_key() {
         let val = vec![
             json!({
                 "Num": 1,
                 "COUNT(*)": 1
             }),
             json!({
                 "Num": 2,
                 "COUNT(*)": 2
             }),
             json!({
                 "Num": 3,
                 "COUNT(*)": 3
             }),
         ];
 
         let out = flatten_objects_for_count(val.clone());
         assert_eq!(val, out);
     }
 }
 