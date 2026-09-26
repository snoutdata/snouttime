//! The background worker (PLAN.md Phase 1.2, D10).
//!
//! It holds no logic of its own: it wakes up, calls `snouttime.run_due_job()` until that
//! says nothing is due, and sleeps again. Each call is its own transaction, so one job's
//! locks are never held across another's, and a job that fails is recorded by the SQL side
//! rather than taking the worker down.
//!
//! ## Which databases it runs in
//!
//! A background worker belongs to one database, and only the postmaster can start one at
//! boot, so the databases are named in `postgresql.conf`:
//!
//! ```conf
//! shared_preload_libraries = 'snouttime'
//! snouttime.databases = 'metrics, analytics'
//! snouttime.interval = '10s'
//! ```
//!
//! Without `shared_preload_libraries` there is no worker at boot. `SELECT
//! snouttime.start_worker()` then starts one for the current database until the server
//! restarts, which is what development and the tests use. Either way the jobs are the same
//! SQL, so a database with no worker is one whose jobs are not being run, not one that
//! behaves differently.

use std::ffi::CString;
use std::time::Duration;

use pgrx::bgworkers::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

/// Databases to run jobs in, comma-separated. Read once, by the postmaster, at startup.
static DATABASES: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// How long to sleep between passes over the job list, in seconds.
static INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(10);

const MAX_WORKERS: usize = 8;

pub fn init() {
	// A PGC_POSTMASTER setting may only be DEFINED while the postmaster is loading its
	// preloaded libraries. Defining one in an ordinary backend is a FATAL error that takes
	// the connection down, so a plain `CREATE EXTENSION snouttime` would disconnect the
	// user who ran it. Everything below the check therefore belongs to preload only.
	let preloaded = unsafe { pg_sys::process_shared_preload_libraries_in_progress };

	GucRegistry::define_int_guc(
		c"snouttime.interval",
		c"Seconds between passes over the SnoutTime job list",
		c"Each pass runs every job that is due, one transaction per job.",
		&INTERVAL,
		1,
		3600,
		GucContext::Sighup,
		GucFlags::default(),
	);

	if !preloaded {
		return;
	}

	GucRegistry::define_string_guc(
		c"snouttime.databases",
		c"Databases SnoutTime runs its jobs in",
		c"Comma-separated. Only read when snouttime is in shared_preload_libraries.",
		&DATABASES,
		GucContext::Postmaster,
		GucFlags::default(),
	);

	for database in databases() {
		BackgroundWorkerBuilder::new(&format!("snouttime: {database}"))
			.set_function("snouttime_worker_main")
			.set_library("snouttime")
			.set_argument(None)
			.set_extra(&database)
			.set_start_time(BgWorkerStartTime::RecoveryFinished)
			// pgrx's default is never to restart. A preloaded worker that dies (the database
			// does not exist yet, or a job took the process down) would then be gone until
			// the server restarts, silently: nothing is left to write to job_runs. A minute
			// is long enough not to flood the log over a database that is never created.
			.set_restart_time(Some(Duration::from_secs(60)))
			.enable_spi_access()
			.load();
	}
}

fn databases() -> Vec<String> {
	DATABASES
		.get()
		.map(|list| {
			list.to_string_lossy()
				.split(',')
				.map(|name| name.trim().to_string())
				.filter(|name| !name.is_empty())
				.take(MAX_WORKERS)
				.collect()
		})
		.unwrap_or_default()
}

/// Start a worker for the current database, for as long as this server runs.
///
/// Superuser only: a background worker connects with no user of its own, and
/// `run_due_job()` is what decides whose privileges each job actually gets.
#[pg_extern]
fn start_worker() -> bool {
	if unsafe { !pgrx::pg_sys::superuser() } {
		error!("only a superuser may start a SnoutTime worker");
	}
	let database = Spi::get_one::<String>("SELECT current_database()::text")
		.expect("current_database()")
		.expect("current_database()");
	BackgroundWorkerBuilder::new(&format!("snouttime: {database}"))
		.set_function("snouttime_worker_main")
		.set_library("snouttime")
		.set_argument(None)
		.set_extra(&database)
		.set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
		.enable_spi_access()
		.load_dynamic()
		.is_ok()
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn snouttime_worker_main(_arg: pg_sys::Datum) {
	BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
	let database = BackgroundWorker::get_extra().to_string();
	BackgroundWorker::connect_worker_to_spi(Some(&database), None);

	log!("SnoutTime worker running jobs in {database}");

	// A preloaded worker starts with the server, which is normally BEFORE anyone has run
	// CREATE EXTENSION snouttime in its database. Calling run_due_job() then is an ERROR
	// that ended the worker for good (found 2026-09-23 by tests/soak/soak.sh, the first
	// thing ever to start one this way). So each pass first asks whether the extension is
	// there, waits quietly while it is not, and says once in the log that it is waiting.
	let mut waiting = false;
	while BackgroundWorker::wait_latch(Some(Duration::from_secs(INTERVAL.get().max(1) as u64))) {
		let installed = BackgroundWorker::transaction(|| {
			Spi::get_one::<bool>(
				"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'snouttime')",
			)
			.unwrap_or(Some(false))
			.unwrap_or(false)
		});
		if !installed {
			if !waiting {
				log!("SnoutTime worker in {database} is waiting for CREATE EXTENSION snouttime");
				waiting = true;
			}
			continue;
		}
		if waiting {
			log!("SnoutTime is installed in {database}; the worker is running its jobs");
			waiting = false;
		}
		// Nothing here decides anything: run_due_job() takes the most overdue job with
		// FOR UPDATE SKIP LOCKED, runs it as its table's owner, and records it.
		loop {
			let more = BackgroundWorker::transaction(|| {
				Spi::get_one::<bool>("SELECT snouttime.run_due_job()")
					.unwrap_or(Some(false))
					.unwrap_or(false)
			});
			if !more {
				break;
			}
		}
	}

	log!("SnoutTime worker in {database} stopping");
}
