use crate::custom_tasks::protein_language::ncbi_nr_softlabels_jsonl2redis::TaskNcbiNrSoftlabelsJsonl2Redis;
use crate::custom_tasks::Task; // Bring Task trait into scope to use its methods

mod custom_tasks;
mod readers;
mod writers;
mod utils;
mod errors; // Declare the errors module

#[tokio::main]
async fn main() {
    env_logger::init(); // Initialize logger
    log::info!("Tasks start!");
    let task = TaskNcbiNrSoftlabelsJsonl2Redis::new(); // Call the constructor
    if let Err(e) = task.run().await {
        log::error!("Task execution failed: {:?}", e);
        // Optionally, exit with an error code
        // std::process::exit(1);
    }
    log::info!("Task invocation completed.");
}
