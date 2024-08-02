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

pub mod prom_utils;
pub mod storage;

use crate::{handlers::http::metrics_path, option::CONFIG, stats::FullStats};
use actix_web_prometheus::{PrometheusMetrics, PrometheusMetricsBuilder};
use clokwerk::{AsyncScheduler, Interval};
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};
use std::{path::Path, time::Duration};
use sysinfo::{Disks, System};

pub const METRICS_NAMESPACE: &str = env!("CARGO_PKG_NAME");
const SYSTEM_METRICS_INTERVAL_SECONDS: Interval = clokwerk::Interval::Minutes(1);

pub static EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested", "Events ingested").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested_size", "Events ingested size bytes")
            .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("storage_size", "Storage size bytes").namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_DELETED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_deleted", "Events deleted").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_DELETED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_deleted_size", "Events deleted size bytes").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static DELETED_EVENTS_STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "deleted_events_storage_size",
            "Deleted events storage size bytes",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("lifetime_events_ingested", "Lifetime events ingested")
            .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_INGESTED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "lifetime_events_ingested_size",
            "Lifetime events ingested size bytes",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "lifetime_events_storage_size",
            "Lifetime events storage size bytes",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_DATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_ingested_date",
            "Events ingested on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_SIZE_DATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_ingested_size_date",
            "Events ingested size in bytes on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static EVENTS_STORAGE_SIZE_DATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_storage_size_date",
            "Events storage size in bytes on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static STAGING_FILES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("staging_files", "Active Staging files").namespace(METRICS_NAMESPACE),
        &["stream"],
    )
    .expect("metric can be created")
});

pub static QUERY_EXECUTE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new("query_execute_time", "Query execute time").namespace(METRICS_NAMESPACE),
        &["stream"],
    )
    .expect("metric can be created")
});

pub static QUERY_CACHE_HIT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("QUERY_CACHE_HIT", "Full Cache hit").namespace(METRICS_NAMESPACE),
        &["stream"],
    )
    .expect("metric can be created")
});

pub static ALERTS_STATES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("alerts_states", "Alerts States").namespace(METRICS_NAMESPACE),
        &["stream", "name", "state"],
    )
    .expect("metric can be created")
});

pub static TOTAL_DISK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("total_disk", "Total Disk Size").namespace(METRICS_NAMESPACE),
        &["volume"],
    )
    .expect("metric can be created")
});

pub static USED_DISK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("used_disk", "Used Disk Size").namespace(METRICS_NAMESPACE),
        &["volume"],
    )
    .expect("metric can be created")
});

pub static AVAILABLE_DISK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("available_disk", "Available Disk Size").namespace(METRICS_NAMESPACE),
        &["volume"],
    )
    .expect("metric can be created")
});

pub static MEMORY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("memory", "Memory").namespace(METRICS_NAMESPACE),
        &["memory"],
    )
    .expect("metric can be created")
});

fn custom_metrics(registry: &Registry) {
    registry
        .register(Box::new(EVENTS_INGESTED.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STORAGE_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_DELETED.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_DELETED_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(DELETED_EVENTS_STORAGE_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(LIFETIME_EVENTS_INGESTED.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(LIFETIME_EVENTS_INGESTED_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(LIFETIME_EVENTS_STORAGE_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_SIZE_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_STORAGE_SIZE_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STAGING_FILES.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(QUERY_EXECUTE_TIME.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(QUERY_CACHE_HIT.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(ALERTS_STATES.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_DISK.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(USED_DISK.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(AVAILABLE_DISK.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(MEMORY.clone()))
        .expect("metric can be registered");
}

pub fn build_metrics_handler() -> PrometheusMetrics {
    let registry = prometheus::Registry::new();
    custom_metrics(&registry);

    let prometheus = PrometheusMetricsBuilder::new(METRICS_NAMESPACE)
        .registry(registry)
        .endpoint(metrics_path().as_str())
        .build()
        .expect("Prometheus initialization");

    prom_process_metrics(&prometheus);
    prometheus
}

#[cfg(target_os = "linux")]
fn prom_process_metrics(metrics: &PrometheusMetrics) {
    use prometheus::process_collector::ProcessCollector;
    metrics
        .registry
        .register(Box::new(ProcessCollector::for_self()))
        .expect("metric can be registered");
}

#[cfg(not(target_os = "linux"))]
fn prom_process_metrics(_metrics: &PrometheusMetrics) {}

pub async fn fetch_stats_from_storage(stream_name: &str, stats: FullStats) {
    EVENTS_INGESTED
        .with_label_values(&[stream_name, "json"])
        .set(stats.current_stats.events as i64);
    EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json"])
        .set(stats.current_stats.ingestion as i64);
    STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .set(stats.current_stats.storage as i64);
    EVENTS_DELETED
        .with_label_values(&[stream_name, "json"])
        .set(stats.deleted_stats.events as i64);
    EVENTS_DELETED_SIZE
        .with_label_values(&[stream_name, "json"])
        .set(stats.deleted_stats.ingestion as i64);
    DELETED_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .set(stats.deleted_stats.storage as i64);

    LIFETIME_EVENTS_INGESTED
        .with_label_values(&[stream_name, "json"])
        .set(stats.lifetime_stats.events as i64);
    LIFETIME_EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json"])
        .set(stats.lifetime_stats.ingestion as i64);
    LIFETIME_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .set(stats.lifetime_stats.storage as i64);
}

pub fn init_system_info_metrics_schedular() -> anyhow::Result<()> {
    log::info!("Setting up schedular for capturing system metrics");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(SYSTEM_METRICS_INTERVAL_SECONDS)
        .run(move || async {
            let result: anyhow::Result<()> = async {
                if let (
                    Some(staging_volume_total_disk),
                    Some(staging_volume_used_disk),
                    Some(staging_volume_available_disk),
                ) = get_volume_disk_usage(CONFIG.staging_dir())
                {
                    TOTAL_DISK
                        .with_label_values(&["staging"])
                        .set(staging_volume_total_disk as i64);
                    USED_DISK
                        .with_label_values(&["staging"])
                        .set(staging_volume_used_disk as i64);
                    AVAILABLE_DISK
                        .with_label_values(&["staging"])
                        .set(staging_volume_available_disk as i64);
                }

                if let (Some(memory), Some(used_memory), Some(total_swap), Some(used_swap)) =
                    get_memory_usage()
                {
                    MEMORY
                        .with_label_values(&["total_memory"])
                        .set(memory as i64);
                    MEMORY
                        .with_label_values(&["used_memory"])
                        .set(used_memory as i64);
                    MEMORY
                        .with_label_values(&["total_swap"])
                        .set(total_swap as i64);
                    MEMORY
                        .with_label_values(&["used_swap"])
                        .set(used_swap as i64);
                }
                Ok(())
            }
            .await;

            if let Err(err) = result {
                log::error!("Error in capturing system metrics: {:?}", err);
            }
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Ok(())
}

pub fn get_volume_disk_usage(path: &Path) -> (Option<u64>, Option<u64>, Option<u64>) {
    let mut disks = Disks::new_with_refreshed_list();
    disks.sort_by_key(|disk| disk.mount_point().to_str().unwrap().len());
    disks.reverse();

    for disk in disks.iter() {
        if path.starts_with(disk.mount_point().to_str().unwrap()) {
            return (
                Some(disk.total_space()),
                Some(disk.available_space()),
                Some(disk.total_space() - disk.available_space()),
            );
        }
    }
    (None, None, None)
}

pub fn get_memory_usage() -> (Option<u64>, Option<u64>, Option<u64>, Option<u64>) {
    let sys = System::new_all();
    let memory = sys.total_memory();
    let used_memory = sys.used_memory();
    let total_swap = sys.total_swap();
    let used_swap = sys.used_swap();
    (
        Some(memory),
        Some(used_memory),
        Some(total_swap),
        Some(used_swap),
    )
}
