use originium::custom_tasks::Task;
use originium::custom_tasks::protein_language::ncbi_nr_eukaryota_fasta2redis::TaskNcbiNrEukaryotaFastaToRedis;
use originium::custom_tasks::protein_language::ncbi_nr_singletons_tsv2redis::TaskNcbiNrSingletonsTsvToRedis;
use originium::custom_tasks::protein_language::ncbi_nr_softlabels_jsonl2redis::TaskNcbiNrSoftlabelsJsonl2Redis;
use originium::custom_tasks::protein_language::ncbi_nr_mixture_redis2redis::TaskNcbiNrMixtureRedisToRedis;
use originium::custom_tasks::protein_language::ncbi_nr_softlabels_redis2mmap::TaskNcbiNrSoftlabelsRedis2Mmap;

#[tokio::main]
async fn main() {
    // Task 1: eukaryota_fasta2redis
    println!("Running TaskNcbiNrEukaryotaFastaToRedis...");
    if let Err(e) = TaskNcbiNrEukaryotaFastaToRedis::new().run().await {
        eprintln!("TaskNcbiNrEukaryotaFastaToRedis failed: {:?}", e);
    } else {
        println!("TaskNcbiNrEukaryotaFastaToRedis completed.");
    }

    // Task 2: singletons_tsv2redis (requires async init)
    println!("Running TaskNcbiNrSingletonsTsvToRedis...");
    match TaskNcbiNrSingletonsTsvToRedis::new().await {
        Ok(task_instance) => {
            if let Err(e) = task_instance.run().await {
                eprintln!("TaskNcbiNrSingletonsTsvToRedis failed: {:?}", e);
            } else {
                println!("TaskNcbiNrSingletonsTsvToRedis completed.");
            }
        }
        Err(e) => {
            eprintln!("Failed to initialize TaskNcbiNrSingletonsTsvToRedis: {:?}", e);
        }
    }

    // Task 3: softlabels_jsonl2redis
    println!("Running TaskNcbiNrSoftlabelsJsonl2Redis...");
    if let Err(e) = TaskNcbiNrSoftlabelsJsonl2Redis::new().run().await {
        eprintln!("TaskNcbiNrSoftlabelsJsonl2Redis failed: {:?}", e);
    } else {
        println!("TaskNcbiNrSoftlabelsJsonl2Redis completed.");
    }

    // Task 4: mixture_redis2redis
    println!("Running TaskNcbiNrMixtureRedisToRedis...");
    if let Err(e) = TaskNcbiNrMixtureRedisToRedis::new().run().await {
        eprintln!("TaskNcbiNrMixtureRedisToRedis failed: {:?}", e);
    } else {
        println!("TaskNcbiNrMixtureRedisToRedis completed.");
    }

    // Task 5: softlabels_redis2mmap
    println!("Running TaskNcbiNrSoftlabelsRedis2Mmap...");
    if let Err(e) = TaskNcbiNrSoftlabelsRedis2Mmap::new().run().await {
        eprintln!("TaskNcbiNrSoftlabelsRedis2Mmap failed: {:?}", e);
    } else {
        println!("TaskNcbiNrSoftlabelsRedis2Mmap completed.");
    }

    println!("All protein language tasks finished.");
} 