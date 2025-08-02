use originium::custom_tasks::natural_language::block_blm_jsonl2mmap::TaskBlockBLMJsonl2Mmap;
use originium::custom_tasks::natural_language::block_blm_jsonl2freq::TaskBlockBLMJsonl2Freq;
use originium::custom_tasks::natural_language::rwkv_jsonl2mmap::TaskRwkvJsonl2Mmap;
use originium::custom_tasks::natural_language::rwkv_mmap2debug::TaskRwkvMmap2Debug;
use originium::custom_tasks::Task;

#[tokio::main]
async fn main() {
    // Task 1: blockblm_jsonl2mmap
    // println!("Running TaskBlockBLMJsonl2Mmap...");
    // if let Err(e) = TaskBlockBLMJsonl2Mmap::new().run().await {
    //     eprintln!("TaskBlockBLMJsonl2Mmap failed: {:?}", e);
    // } else {
    //     println!("TaskBlockBLMJsonl2Mmap completed.");
    // }

    // Task 2: blockblm_jsonl2freq
    // println!("Running TaskBlockBLMJsonl2Freq...");
    // if let Err(e) = TaskBlockBLMJsonl2Freq::new().run().await {
    //     eprintln!("TaskBlockBLMJsonl2Freq failed: {:?}", e);
    // } else {
    //     println!("TaskBlockBLMJsonl2Freq completed.");
    // }

    // // Task 3: rwkv_jsonl2mmap
    println!("Running TaskRwkvJsonl2Mmap...");
    if let Err(e) = TaskRwkvJsonl2Mmap::new().run().await {
        eprintln!("TaskRwkvJsonl2Mmap failed: {:?}", e);
    } else {
        println!("TaskRwkvJsonl2Mmap completed.");
    }
    //
    // // Task 4: rwkv_mmap2debug
    // println!("Running TaskRwkvMmap2Debug...");
    // if let Err(e) = TaskRwkvMmap2Debug::new().run().await {
    //     eprintln!("TaskRwkvMmap2Debug failed: {:?}", e);
    // } else {
    //     println!("TaskRwkvMmap2Debug completed.");
    // }

    println!("All natural language tasks finished.");
} 