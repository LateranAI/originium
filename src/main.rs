use crate::custom_tasks::protein_language::ncbi_nr_softlabels_jsonl2redis::TaskNcbiNrSoftlabelsJsonl2Redis;
use crate::custom_tasks::Task;

mod custom_tasks;
mod readers;
mod writers;
mod utils;
mod errors;

const TEST_MODE: bool = false;

#[tokio::main]
async fn main() {
    println!("Tasks start!");
    let task = TaskNcbiNrSoftlabelsJsonl2Redis::new();
    if let Err(e) = task.run().await {
        eprintln!("Task execution failed: {:?}", e);
    }
    println!("Task invocation completed.");
}
