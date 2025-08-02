use originium::custom_tasks::protein_language::ncbi_genome::mixture_redis2redis::TaskNcbiGenomeMixtureRedisToRedis;
use originium::custom_tasks::protein_language::ncbi_genome::singletons_fasta2redis::TaskNcbiGenomeSingletonsFastaToRedis;
use originium::custom_tasks::protein_language::ncbi_genome::softlabels_jsonl2redis::TaskNcbiGenomeSoftlabelsJsonl2Redis;
use originium::custom_tasks::protein_language::ncbi_genome::softlabels_redis2mmap::TaskNcbiGenomeSoftlabelsRedis2Mmap;
use originium::custom_tasks::Task;

#[tokio::main]
async fn main() {
    // Task 1: singletons_fasta2redis
    // println!("Running TaskNcbiGenomeSingletonsFastaToRedis...");
    // if let Err(e) = TaskNcbiGenomeSingletonsFastaToRedis::new().run().await {
    //     eprintln!("TaskNcbiGenomeSingletonsFastaToRedis failed: {:?}", e);
    // } else {
    //     println!("TaskNcbiGenomeSingletonsFastaToRedis completed.");
    // }

    // Task 2: softlabels_jsonl2redis
    println!("Running TaskNcbiGenomeSoftlabelsJsonl2Redis...");
    if let Err(e) = TaskNcbiGenomeSoftlabelsJsonl2Redis::new().run().await {
        eprintln!("TaskNcbiGenomeSoftlabelsJsonl2Redis failed: {:?}", e);
    } else {
        println!("TaskNcbiGenomeSoftlabelsJsonl2Redis completed.");
    }

    // Task 3: mixture_redis2redis
    println!("Running TaskNcbiGenomeMixtureRedisToRedis...");
    if let Err(e) = TaskNcbiGenomeMixtureRedisToRedis::new().run().await {
        eprintln!("TaskNcbiGenomeMixtureRedisToRedis failed: {:?}", e);
    } else {
        println!("TaskNcbiGenomeMixtureRedisToRedis completed.");
    }

    // Task 4: softlabels_redis2mmap
    println!("Running TaskNcbiGenomeSoftlabelsRedis2Mmap...");
    if let Err(e) = TaskNcbiGenomeSoftlabelsRedis2Mmap::new().run().await {
        eprintln!("TaskNcbiGenomeSoftlabelsRedis2Mmap failed: {:?}", e);
    } else {
        println!("TaskNcbiGenomeSoftlabelsRedis2Mmap completed.");
    }

    println!("All protein language tasks finished.");
}