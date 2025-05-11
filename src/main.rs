use crate::custom_tasks::natural_language::rwkv_jsonl2mmap::TaskRwkvJsonl2Mmap;
use crate::custom_tasks::natural_language::rwkv_mmap2debug::TaskRwkvMmap2Debug;
use crate::custom_tasks::protein_language::ncbi_nr_singletons_tsv2redis::TaskNcbiNrSingletonsTsvToRedis;

use crate::custom_tasks::protein_language::ncbi_nr_eukaryota_fasta2redis::TaskNcbiNrEukaryotaFastaToRedis;
use crate::custom_tasks::protein_language::ncbi_nr_softlabels_jsonl2redis::TaskNcbiNrSoftlabelsJsonl2Redis;
use crate::custom_tasks::Task;

mod custom_tasks;
mod errors;
mod readers;
mod utils;
mod writers;

const TEST_MODE: bool = false;

#[tokio::main]
async fn main() {
    println!("Tasks start!");

    // println!("Running Task NcbiNr Eukaryota Fasta To Redis...");
    // let eukaryota_task = TaskNcbiNrEukaryotaFastaToRedis::new();
    // if let Err(e) = eukaryota_task.run().await {
    //     eprintln!("Task NcbiNr Eukaryota Fasta To Redis execution failed: {:?}", e);
    // } else {
    //     println!("Task NcbiNr Eukaryota Fasta To Redis completed.");
    // }
    // println!("Task NcbiNr Eukaryota Fasta To Redis invocation attempt finished.");
    //
    // println!("Running Task NcbiNr Softlabels Jsonl To Redis...");
    // let task_softlabel = TaskNcbiNrSoftlabelsJsonl2Redis::new();
    // if let Err(e) = task_softlabel.run().await {
    //     eprintln!(
    //         "Task NcbiNr Softlabels Jsonl To Redis execution failed: {:?}",
    //         e
    //     );
    // }
    // println!("Task NcbiNr Softlabels Jsonl To Redis invocation completed.");
    //
    // println!("Running Task NcbiNr Singletons Tsv To Redis...");
    // let task_singletons = TaskNcbiNrSingletonsTsvToRedis::new().await.unwrap();
    // if let Err(e) = task_singletons.run().await {
    //     eprintln!(
    //         "Task NcbiNr Singletons Tsv To Redis execution failed: {:?}",
    //         e
    //     );
    // }
    // println!("Task NcbiNr Singletons Tsv To Redis invocation completed.");

    println!("Running Task RWKV Jsonl To Mmap...");
    let task_rwkv = TaskRwkvJsonl2Mmap::new();
    if let Err(e) = task_rwkv.run().await {
        eprintln!(
            "Task RWKV Jsonl To Mmap execution failed: {:?}",
            e
        );
    }
    println!("Task RWKV Jsonl To Mmap invocation completed.");

    println!("Running Task RWKV Mmap To Debug...");
    let task_rwkv = TaskRwkvMmap2Debug::new();
    if let Err(e) = task_rwkv.run().await {
        eprintln!(
            "Task RWKV Mmap To Debug execution failed: {:?}",
            e
        );
    }
    println!("Task RWKV Mmap To Debug invocation completed.");

}
