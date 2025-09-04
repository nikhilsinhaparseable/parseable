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

use chrono::{NaiveDateTime, Utc};
use prometheus_parse::{Sample as PromSample, Value as PromValue};
use serde::Serialize;
use std::collections::HashMap;

use crate::{handlers::http::ingest::PostError, handlers::http::modal::Metadata};

#[derive(Debug, Serialize, Clone)]
pub struct BillingMetrics {
    address: String,
    node_type: String,
    parseable_total_events_ingested_by_date: HashMap<String, u64>,
    parseable_total_events_ingested_size_by_date: HashMap<String, u64>,
    parseable_total_parquets_stored_by_date: HashMap<String, u64>,
    parseable_total_parquets_stored_size_by_date: HashMap<String, u64>,
    parseable_total_query_calls_by_date: HashMap<String, u64>,
    parseable_total_files_scanned_in_query_by_date: HashMap<String, u64>,
    parseable_total_bytes_scanned_in_query_by_date: HashMap<String, u64>,
    parseable_total_object_store_calls_by_date: HashMap<String, u64>,
    parseable_total_files_scanned_in_object_store_calls_by_date: HashMap<String, u64>,
    event_type: String,
    event_time: NaiveDateTime,
}

impl Default for BillingMetrics {
    fn default() -> Self {
        Self {
            address: "".to_string(),
            node_type: "".to_string(),
            parseable_total_events_ingested_by_date: HashMap::new(),
            parseable_total_events_ingested_size_by_date: HashMap::new(),
            parseable_total_parquets_stored_by_date: HashMap::new(),
            parseable_total_parquets_stored_size_by_date: HashMap::new(),
            parseable_total_query_calls_by_date: HashMap::new(),
            parseable_total_files_scanned_in_query_by_date: HashMap::new(),
            parseable_total_bytes_scanned_in_query_by_date: HashMap::new(),
            parseable_total_object_store_calls_by_date: HashMap::new(),
            parseable_total_files_scanned_in_object_store_calls_by_date: HashMap::new(),
            event_type: "billing-metrics".to_string(),
            event_time: Utc::now().naive_utc(),
        }
    }
}

impl BillingMetrics {
    fn new(address: String, node_type: String) -> Self {
        Self {
            address,
            node_type,
            parseable_total_events_ingested_by_date: HashMap::new(),
            parseable_total_events_ingested_size_by_date: HashMap::new(),
            parseable_total_parquets_stored_by_date: HashMap::new(),
            parseable_total_parquets_stored_size_by_date: HashMap::new(),
            parseable_total_query_calls_by_date: HashMap::new(),
            parseable_total_files_scanned_in_query_by_date: HashMap::new(),
            parseable_total_bytes_scanned_in_query_by_date: HashMap::new(),
            parseable_total_object_store_calls_by_date: HashMap::new(),
            parseable_total_files_scanned_in_object_store_calls_by_date: HashMap::new(),
            event_type: "billing-metrics".to_string(),
            event_time: Utc::now().naive_utc(),
        }
    }

    pub async fn from_prometheus_samples<T: Metadata>(
        samples: Vec<PromSample>,
        metadata: &T,
    ) -> Result<Self, PostError> {
        let mut billing_metrics = BillingMetrics::new(
            metadata.domain_name().to_string(),
            metadata.node_type().to_string(),
        );

        for sample in samples {
            if let PromValue::Counter(val) = sample.value {
                let date = sample
                    .labels
                    .get("date")
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                match sample.metric.as_str() {
                    "parseable_total_events_ingested_by_date" => {
                        *billing_metrics
                            .parseable_total_events_ingested_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_events_ingested_size_by_date" => {
                        *billing_metrics
                            .parseable_total_events_ingested_size_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_parquets_stored_by_date" => {
                        *billing_metrics
                            .parseable_total_parquets_stored_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_parquets_stored_size_by_date" => {
                        *billing_metrics
                            .parseable_total_parquets_stored_size_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_query_calls_by_date" => {
                        *billing_metrics
                            .parseable_total_query_calls_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_files_scanned_in_query_by_date" => {
                        *billing_metrics
                            .parseable_total_files_scanned_in_query_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_bytes_scanned_in_query_by_date" => {
                        *billing_metrics
                            .parseable_total_bytes_scanned_in_query_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_object_store_calls_by_date" => {
                        *billing_metrics
                            .parseable_total_object_store_calls_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    "parseable_total_files_scanned_in_object_store_calls_by_date" => {
                        *billing_metrics
                            .parseable_total_files_scanned_in_object_store_calls_by_date
                            .entry(date)
                            .or_insert(0) += val as u64;
                    }
                    _ => {}
                }
            }
        }

        Ok(billing_metrics)
    }

    /// Sum all billing metrics from multiple nodes
    pub fn sum_metrics(metrics_list: Vec<BillingMetrics>) -> BillingMetrics {
        let mut combined_metrics = BillingMetrics::default();

        if let Some(first_metrics) = metrics_list.first() {
            combined_metrics.event_type = first_metrics.event_type.clone();
            combined_metrics.event_time = first_metrics.event_time;
        }

        for metrics in metrics_list {
            // Sum events ingested by date
            for (date, count) in metrics.parseable_total_events_ingested_by_date {
                *combined_metrics
                    .parseable_total_events_ingested_by_date
                    .entry(date)
                    .or_insert(0) += count;
            }

            // Sum events ingested size by date
            for (date, size) in metrics.parseable_total_events_ingested_size_by_date {
                *combined_metrics
                    .parseable_total_events_ingested_size_by_date
                    .entry(date)
                    .or_insert(0) += size;
            }

            // Sum parquets stored by date
            for (date, count) in metrics.parseable_total_parquets_stored_by_date {
                *combined_metrics
                    .parseable_total_parquets_stored_by_date
                    .entry(date)
                    .or_insert(0) += count;
            }

            // Sum parquets stored size by date
            for (date, size) in metrics.parseable_total_parquets_stored_size_by_date {
                *combined_metrics
                    .parseable_total_parquets_stored_size_by_date
                    .entry(date)
                    .or_insert(0) += size;
            }

            // Sum query calls by date
            for (date, count) in metrics.parseable_total_query_calls_by_date {
                *combined_metrics
                    .parseable_total_query_calls_by_date
                    .entry(date)
                    .or_insert(0) += count;
            }

            // Sum files scanned in query by date
            for (date, count) in metrics.parseable_total_files_scanned_in_query_by_date {
                *combined_metrics
                    .parseable_total_files_scanned_in_query_by_date
                    .entry(date)
                    .or_insert(0) += count;
            }

            // Sum bytes scanned in query by date
            for (date, bytes) in metrics.parseable_total_bytes_scanned_in_query_by_date {
                *combined_metrics
                    .parseable_total_bytes_scanned_in_query_by_date
                    .entry(date)
                    .or_insert(0) += bytes;
            }

            // Sum object store calls by date
            for (date, count) in metrics.parseable_total_object_store_calls_by_date {
                *combined_metrics
                    .parseable_total_object_store_calls_by_date
                    .entry(date)
                    .or_insert(0) += count;
            }

            // Sum files scanned in object store calls by date
            for (date, count) in metrics.parseable_total_files_scanned_in_object_store_calls_by_date
            {
                *combined_metrics
                    .parseable_total_files_scanned_in_object_store_calls_by_date
                    .entry(date)
                    .or_insert(0) += count;
            }
        }

        combined_metrics
    }
}
