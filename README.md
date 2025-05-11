# Originium Data Framework: 高效异步数据处理流水线

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/LateranAI/originium)

**Originium** 是一个基于 Rust 构建的、高度可扩展的异步数据处理框架。它旨在简化和加速IO密集型任务的开发，通过提供一个清晰的流水线模型，将数据读取、处理和写入解耦，充分利用异步操作的性能优势。

## 核心理念

Originium 的设计哲学围绕以下几个核心原则：

1.  **异步优先 (Async First)**：充分利用 Rust 的 `async/await` 特性，实现高并发的IO操作，最大化系统吞吐量。
2.  **模块化与可扩展性 (Modularity & Extensibility)**：通过定义清晰的 `Task`, `Reader`, 和 `Writer` trait，用户可以轻松接入新的数据源、处理逻辑和输出目标。
3.  **类型安全 (Type Safety)**： leveraging Rust的强类型系统，确保数据在处理流程中的一致性和正确性。
4.  **开发者友好 (Developer-Friendly)**：提供简洁的API和明确的模式，降低开发复杂数据处理任务的门槛。
5.  **高性能 (High Performance)**：专注于IO密集型场景，通过并行处理和高效的资源管理，追求极致的执行效率。

## 解决的问题

在数据驱动的时代，我们经常面临以下挑战：

*   **多样化的数据源和目标**：数据可能来自文件（JSONL, CSV, XML, FASTA）、数据库（PostgreSQL, MySQL）、消息队列或实时流，并需要输出到类似多样的系统中。
*   **复杂的处理逻辑**：原始数据往往需要清洗、转换、富化或与其他数据关联。
*   **IO瓶颈**：传统同步IO操作容易成为性能瓶颈，尤其是在处理大规模数据集时。
*   **重复的脚手架代码**：为每个数据处理任务搭建读取、并发控制、写入等基础设施费时费力。

Originium 旨在提供一个统一的解决方案，抽象化底层的IO操作和并发管理，让开发者能更专注于核心的业务逻辑。

## 适用任务

Originium 特别适用于**IO密集型任务**，例如：

*   大规模数据集的格式转换（如 JSONL 转 Redis, FASTA 转自定义二进制/内存映射索引格式 (Mmap)）。
*   数据清洗、规范化和预处理。
*   从多种数据源聚合信息并写入数据库。
*   实时数据流的初步处理和分发。

它可能**不适合**纯粹的计算密集型任务（如复杂的数值模拟或机器学习模型训练本身），这些任务通常需要专门的计算库和不同的并行策略。

## 框架核心组件

### 1. `Task` Trait

`Task` 是 Originium 框架的核心抽象。每个具体的数据处理流程都应实现此 trait。

```rust
#[async_trait::async_trait]
pub trait Task: Clone + Send + Sync + 'static {
    // 定义任务输入的数据单元类型（通常为结构体或枚举）
    type ReadItem: Send
        + Sync
        + 'static
        + Debug
        + Clone
        + DeserializeOwned
        + Unpin
        + for<'r> FromRow<'r, AnyRow>;
    // 定义任务处理后输出的数据单元类型
    type ProcessedItem: Send + Sync + 'static + Debug + Clone + Serialize + Display;

    // 声明任务需要的输入数据端点
    fn get_inputs_info() -> Vec<DataEndpoint>;
    // 声明任务期望的输出数据端点
    fn get_outputs_info() -> Vec<DataEndpoint>;

    // 定义如何从 InputItem 解析为 ReadItem
    fn read(&self) -> Box<dyn Fn(InputItem) -> Self::ReadItem + Send + Sync + 'static>;
    
    // 定义核心处理逻辑：ReadItem -> Option<ProcessedItem>
    async fn process(
        &self,
        item: Self::ReadItem,
    ) -> Result<Option<Self::ProcessedItem>, FrameworkError>;

    // 根据配置获取数据写入器实例
    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError>;
    
    // 任务执行的入口点（框架已提供默认实现，该实现包含了一个动态并发调整机制）
    async fn run(&self) -> Result<(), FrameworkError>;
}
```

**`Task` 配置项说明：**

一般来说, 除了process函数包含自定义的处理逻辑可能较长之外, 其他配置项不会超过十行.

*   `ReadItem`: 定义进入处理流程的单个数据项的类型。
    *   **重要约束**: 此类型必须实现 `serde::de::DeserializeOwned` (以便可以从多种来源反序列化) 和 `sqlx::FromRow<'r, sqlx::any::AnyRow>` (以便能从任何支持的SQL数据库行进行映射)。
*   `ProcessedItem`: 定义经过 `process` 方法处理后的数据项类型。
    *   **约束**: 必须实现 `serde::Serialize` (以便可以序列化到多种输出) 和 `std::fmt::Display` (用于调试和日志)。
*   `get_inputs_info() -> Vec<DataEndpoint>`: 返回一个 `DataEndpoint` 枚举的向量，声明此任务从哪些数据源读取数据。
*   `get_outputs_info() -> Vec<DataEndpoint>`: 返回一个 `DataEndpoint` 枚举的向量，声明此任务将数据写入哪些目标。
*   `read(&self) -> Box<dyn Fn(InputItem) -> Self::ReadItem>`:
    *   **角色**: 将从各种Reader获得的`InputItem`转换为任务特定的`ReadItem`类型。
    *   **注意**: 闭包需要处理所有可能的`InputItem`变体，不匹配的情况通常用panic处理。
*   `process(&self, item: Self::ReadItem) -> Result<Option<Self::ProcessedItem>, FrameworkError>`:
    *   **角色**: 这是任务的核心业务逻辑。它异步处理一个`ReadItem`，并返回一个`Result<Option<Self::ProcessedItem>>`。
    *   返回`Ok(None)`表示该项被过滤，`Err`表示处理错误，`Ok(Some(item))`表示成功处理的结果。
*   `get_writer(...)`: 框架通过此方法为每个输出 `DataEndpoint` 获取相应的 `Writer` 实例。

### 2. `InputItem` 与多类型支持

`InputItem` 是一个枚举类型，支持多种输入数据结构，便于不同Reader/数据源的统一流式处理：

```rust
#[derive(Debug)]
pub enum InputItem {
    String(String),           // 用于文本类数据
    FastaItem(FastaItem),     // 用于FASTA格式数据
    // 可扩展更多类型
}
```
- 各Reader会将原始数据包装为对应的`InputItem`变体。
- `Task::read`闭包负责将`InputItem`转换为具体的`ReadItem`。
- 这种设计支持跨数据源类型的统一处理流程。

### 3. 多输入端点自动合并机制

- 框架支持`get_inputs_info()`返回多个`DataEndpoint`（如多个FASTA文件、多个数据库表等）。
- 框架会为每个输入端点自动创建Reader，所有Reader读取的数据会通过MPSC通道合并到主broker。
- 后续的`process`和`writer`阶段对所有输入数据统一处理，无需关心数据来源。
- 这种机制特别适用于多文件批量处理、数据聚合等场景。
- 例如，可以同时从多个FASTA文件读取序列数据，或者从多个Redis数据库同时获取键值对。

### 4. `DataEndpoint` 枚举

`DataEndpoint` 用于统一描述各种数据源和数据汇。框架会根据此枚举的变体自动选择和配置合适的 `Reader` 或 `Writer`。

目前支持的端点类型包括：
*   `LineDelimited { path: String, format: LineFormat }`: 通用行式文件读写，通过 `format` (如 `Jsonl`, `Tsv`, `PlainText`) 指定具体格式。
*   `Xml { path: String }`: XML 文件 (需要配置记录标签)。
*   `Fasta { path: String }`: FASTA/FASTQ 文件。
*   `Postgres { url: String, table: String }`: PostgreSQL 数据库表 (读取时 `table` 用于 `SELECT * FROM table`, 写入时指定目标表)。
*   `MySQL { url: String, table: String }`: MySQL 数据库表。
*   `Redis { url: String, key_prefix: String, max_concurrent_tasks: usize }`: Redis 数据库。
*   `Mmap { base_path: String, filename: String, num_threads: usize, token_unit_type: MmapTokenUnitType, token_unit_len: usize, is_legacy_rwkv_format: bool }`: 用于读写自定义的内存映射二进制索引格式（`.bin` 数据文件和 `.idx` 索引文件）。
    *   `base_path`: `.bin` 和 `.idx` 文件所在的基础目录。
    *   `filename`: 文件名（不含扩展名）。
    *   `num_threads`: 并行处理时使用的线程数。
    *   `token_unit_type`: 定义数据文件中每个最小单元的类型 (例如 `U16` 代表 `u16`, `F32` 代表 `f32`)。
    *   `token_unit_len`: 定义一个逻辑数据项由多少个 `token_unit_type` 单元组成 (例如，对于标准分词，通常为1；对于soft-label等可能大于1)。
    *   `is_legacy_rwkv_format`: 布尔值，若为 `true`，则表示兼容旧版 RWKV 项目的特定 `.idx` 文件头格式 (版本1，DTYPE为 `u16` 类型)；若为 `false`，则使用新的通用格式 (版本2)，其 `.idx` 文件头会包含 `token_unit_type` 和 `token_unit_len` 信息。
*   `Debug { prefix: Option<String> }`: 一个简单的调试写入器，将数据项打印到控制台。

### 5. `Reader` Trait

`Reader` trait 定义了数据读取器的行为。框架内置了对应各种 `DataEndpoint` 的 `Reader` 实现。

```rust
#[async_trait::async_trait]
pub trait Reader<Item>: Send + Sync
where
    Item: Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>, // 由 Task::read() 提供
        mp: Arc<MultiProgress>,
    ) -> tokio::sync::mpsc::Receiver<Item>;
}
```
*   **角色**: `Reader` 负责从其配置的数据源（如文件、数据库）异步读取数据，并通过 `Task` 提供的 `read_fn`（即 `Task::read()` 的返回值）将原始数据（如`String`或`FastaItem`）转换为 `ReadItem`，然后通过一个 MPSC channel 将这些 `ReadItem` 发送到处理阶段。
*   所有Reader实现会自动提供进度条通过`MultiProgress`参数。

### 6. `Writer` Trait

`Writer` trait 定义了数据写入器的行为。框架通过 `Task::get_writer()` 方法获取具体的写入器实例。

```rust
#[async_trait::async_trait]
pub trait Writer<OutputItem: Send + 'static>: Send + Sync {
    async fn pipeline(
        &self,
        rx: tokio::sync::mpsc::Receiver<OutputItem>, // 从处理阶段接收 ProcessedItem
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
```
*   **角色**: `Writer` 从 MPSC channel 接收处理阶段生成的 `ProcessedItem`，并将它们异步写入其配置的数据目标（如文件、数据库）。
*   所有Writer实现会自动提供进度条通过`MultiProgress`参数。

### 7. main.rs任务调度说明

默认情况下，`main.rs`中的多个任务是**串行**执行的：

```rust
#[tokio::main]
async fn main() {
    let task1 = TaskA::new();
    task1.run().await.unwrap();
    let task2 = TaskB::new();
    task2.run().await.unwrap();
    // ...
}
```
- 每个任务`run().await`会等待前一个任务完全结束。

如需**并发执行**，可用`tokio::spawn`或`tokio::join!`：

```rust
#[tokio::main]
async fn main() {
    let t1 = TaskA::new();
    let t2 = TaskB::new();
    let h1 = tokio::spawn(async move { t1.run().await });
    let h2 = tokio::spawn(async move { t2.run().await });
    let _ = tokio::try_join!(h1, h2);
}
```

## `Task` 设计注意事项

设计一个高效且正确的 `Task` 实现时，请考虑以下几点：

1.  **`ReadItem` 与 `FromRow`**：
    *   `Task` trait 要求 `ReadItem` 必须实现 `for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow>`。这是为了确保如果任务配置了 SQL 数据源，框架可以从数据库行中构造出 `ReadItem`。
    *   对于简单的结构体，通常可以通过 `#[derive(sqlx::FromRow)]` 轻松实现。
    *   **支持的字段类型**：与 `#[derive(sqlx::FromRow)]` 兼容的常见字段类型包括：
        *   基本类型：`bool`, `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`, `f32`, `f64`
        *   字符串：`String`
        *   字节序列：`Vec<u8>`
        *   选项：`Option<T>` (其中 `T` 是支持的类型)
        *   `sqlx` 提供的特定类型：如日期时间类型 (`chrono::NaiveDate`, `chrono::NaiveDateTime`, etc.)，`Uuid` 等 (需要启用相应 `sqlx` features)。
    *   **处理复杂类型 (如 `serde_json::Value`)**：
        *   `serde_json::Value` (或 `sqlx::types::Json<serde_json::Value>`) 不能直接与 `sqlx::any::AnyRow` 的 `FromRow` 派生兼容，因为 `sqlx` 没有为 `Json<T>` 提供通用的 `sqlx::Decode<'_, sqlx::Any>` 实现。
        *   **建议策略**：如果您的 `ReadItem` 需要表示复杂结构（如 JSON 对象/数组）并且可能从 SQL 读取，请在数据库中将该列存储为 `TEXT` 类型。在您的 `ReadItem` 结构体中，将对应的字段定义为 `String` 类型。`#[derive(sqlx::FromRow)]` 将能正确处理这个 `String` 字段。然后，在 `Task::process` 方法内部，您可以使用 `serde_json::from_str()` 将这个字符串解析为实际的 `serde_json::Value` 或目标结构体。
        *   **对于非 SQL 输入 (如 JSONL)**:
            *   如果您的主要输入是 JSONL，并且 `ReadItem` 是一个表示整行内容的 `LineInput { content: String }` 这样的简单包装器，那么 `read` 方法会很简单。解析 JSON 字符串的逻辑会移至 `process` 方法。这种方式可以避免不必要的多次序列化/反序列化，并简化 `FromRow` 的满足。

2.  **`read` vs. `process` 的职责**：
    *   `read` 的主要职责是将`InputItem`转换为`ReadItem`。通常是一个简单转换或包装操作。
    *   `process`方法负责复杂的处理逻辑，包括数据验证、转换、聚合等。这是任务的核心业务逻辑所在。
    *   对于可恢复的错误，`process`返回`Err`；对于需要跳过的数据，返回`Ok(None)`；成功处理则返回`Ok(Some(item))`。

3.  **错误处理**：
    *   在 `read` 闭包中，对于不可恢复的错误（如无法处理的`InputItem`变体），通常使用 `panic!`。框架会捕获任务的 panic，但应尽量避免。
    *   在 `process` 方法中，返回`Result`使错误处理更加显式和可控。
    *   `Task::get_writer()` 和 `Writer::pipeline()` 返回 `Result`，允许更细致的错误传递。

4.  **状态管理**：
    *   `Task` 本身是 `&self`，但 `read` 返回的是 `'static` 闭包。如果这些闭包需要访问 `Task` 的状态（如配置、共享资源如连接池或分词器），通常通过 `Arc<T>` 将状态克隆并 `move` 到闭包中。
    *   对于需要在多个任务调用之间共享的可变状态（如计数器），使用 `Arc<AtomicUsize>` 或其他线程安全方案。

5.  **性能考虑**：
    *   最小化 `ReadItem` 和 `ProcessedItem` 的克隆 (`.clone()`)，除非必要。
    *   在 `process` 中避免阻塞操作。如果需要执行 CPU 密集型或可能阻塞的操作，考虑使用 `tokio::task::spawn_blocking`。
    *   对于特定的Reader/Writer组件（如 `RedisReader`, `RedisWriter`）可能仍有其独立的并发参数（如`max_concurrent_tasks`），这些参数控制其组件内部的并发行为。
    *   **自动化并发管理**: 框架的默认 `Task::run` 实现内置了动态并发调整机制，可自动优化核心处理阶段的并行度，以适应不同的IO和处理负载。

---

## 贡献

我们欢迎社区的贡献！无论是功能增强、bug修复、文档改进还是新的 `Reader`/`Writer` 实现，请随时通过 Pull Request 或 Issue 参与进来。