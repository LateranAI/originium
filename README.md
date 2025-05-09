# Originium Data Framework: 高效异步数据处理流水线

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)

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

*   大规模数据集的格式转换（如 JSONL 转 Redis, FASTA 转 Binidx）。
*   数据清洗、规范化和预处理。
*   从多种数据源聚合信息并写入数据库。
*   实时数据流的初步处理和分发。

它可能**不适合**纯粹的计算密集型任务（如复杂的数值模拟或机器学习模型训练本身），这些任务通常需要专门的计算库和不同的并行策略。

## 框架核心组件

### 1. `Task` Trait

`Task` 是 Originium 框架的核心抽象。每个具体的数据处理流程都应实现此 trait。

```rust
#[async_trait::async_trait]
pub trait Task: Send + Sync + 'static {
    // 定义任务输入的数据单元类型
    type InputItem: Send + Sync + 'static + Debug + Clone + DeserializeOwned + Unpin
        + for<'r> FromRow<'r, AnyRow>; 
    
    // 定义任务处理后输出的数据单元类型
    type ProcessedItem: Send + Sync + 'static + Debug + Clone + Serialize + Display;

    // 声明任务需要的输入数据端点
    fn get_inputs_info() -> Vec<DataEndpoint>;
    // 声明任务期望的输出数据端点
    fn get_outputs_info() -> Vec<DataEndpoint>;

    // 定义如何从原始字符串解析为 InputItem (主要用于基于行的文本读取器)
    fn read(&self) -> Box<dyn Fn(String) -> Self::InputItem + Send + Sync + 'static>;
    
    // 定义核心处理逻辑：InputItem -> Option<ProcessedItem>
    fn process(&self) -> Box<dyn Fn(Self::InputItem) -> Option<Self::ProcessedItem> + Send + Sync + 'static>;

    // 根据配置获取数据写入器实例
    async fn get_writer(&self, endpoint_config: &DataEndpoint)
        -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError>;
    
    // 任务执行的入口点（框架已提供默认实现，该实现包含了一个动态并发调整机制。它会根据处理速率自动调整核心处理阶段的并发操作数（初始值基于CPU核心数，处理每批数据后评估性能，若速率显著提升则并发数翻倍，若下降则减少前一次增加量的一半，速率稳定则锁定并发数，上下限通常为CPU核心数的0.5倍至8倍），旨在无需手动配置的情况下优化吞吐量。）
    async fn run(&self) -> Result<(), FrameworkError>;
}
```

**`Task` 配置项说明：**

一般来说, 除了process函数包含自定义的处理逻辑可能较长之外, 其他配置项不会超过十行.

*   `InputItem`: 定义进入处理流程的单个数据项的类型。
    *   **重要约束**: 此类型必须实现 `serde::de::DeserializeOwned` (以便可以从多种来源反序列化) 和 `sqlx::FromRow<'r, sqlx::any::AnyRow>` (以便能从任何支持的SQL数据库行进行映射)。
*   `ProcessedItem`: 定义经过 `process` 方法处理后的数据项类型。
    *   **约束**: 必须实现 `serde::Serialize` (以便可以序列化到多种输出) 和 `std::fmt::Display` (用于调试和日志)。
*   `get_inputs_info() -> Vec<DataEndpoint>`: 返回一个 `DataEndpoint` 枚举的向量，声明此任务从哪些数据源读取数据。
*   `get_outputs_info() -> Vec<DataEndpoint>`: 返回一个 `DataEndpoint` 枚举的向量，声明此任务将数据写入哪些目标。
*   `read(&self) -> Box<dyn Fn(String) -> Self::InputItem>`:
    *   **角色**: 主要用于将从基于行的文本读取器（如 `JsonlReader`, `FileReader`, `TsvReader`）获得的原始 `String` 行转换为结构化的 `InputItem`。
    *   **注意**: 如果 `InputItem` 是 `String` 或简单的包装类型（如 `TextLine { value: String }`），此闭包可以非常简单，例如 `Box::new(|s| s)` 或 `Box::new(|s| TextLine { value: s })`。对于复杂的 `InputItem`，这里会包含解析逻辑。
*   `process(&self) -> Box<dyn Fn(Self::InputItem) -> Option<Self::ProcessedItem>>`:
    *   **角色**: 这是任务的核心业务逻辑。它接收一个 `InputItem`，进行处理，并返回一个 `Option<Self::ProcessedItem>`。返回 `None` 表示该 `InputItem` 在处理后被丢弃或过滤。
*   `get_writer(...)`: 框架通过此方法为每个输出 `DataEndpoint` 获取相应的 `Writer` 实例。

### 2. `DataEndpoint` 枚举

`DataEndpoint` 用于统一描述各种数据源和数据汇。框架会根据此枚举的变体自动选择和配置合适的 `Reader` 或 `Writer`。

目前支持的端点类型包括：
*   `LineDelimited { path: String, format: LineFormat }`: 通用行式文件读写，通过 `format` (如 `Jsonl`, `Tsv`, `PlainText`) 指定具体格式。
*   `Xml { path: String }`: XML 文件 (需要配置记录标签)。
*   `Fasta { path: String }`: FASTA/FASTQ 文件。
*   `Postgres { url: String, table: String }`: PostgreSQL 数据库表 (读取时 `table` 用于 `SELECT * FROM table`, 写入时指定目标表)。
*   `MySQL { url: String, table: String }`: MySQL 数据库表。
*   `Redis { url: String, key_prefix: String, max_concurrent_tasks: usize }`: Redis 数据库。
*   `RwkvBinidx { base_path: String, filename_prefix: String, num_threads: usize }`: RWKV项目的特定binidx格式。
*   `Debug { prefix: Option<String> }`: 一个简单的调试写入器，将数据项打印到控制台。

### 3. `Reader` Trait

`Reader` trait 定义了数据读取器的行为。框架内置了对应各种 `DataEndpoint` 的 `Reader` 实现。

```rust
#[async_trait::async_trait]
pub trait Reader<Item>: Send + Sync
where
    Item: Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(String) -> Item + Send + Sync + 'static>, // 由 Task::read() 提供
    ) -> tokio::sync::mpsc::Receiver<Item>;
}
```
*   **角色**: `Reader` 负责从其配置的数据源（如文件、数据库）异步读取数据，并通过 `Task` 提供的 `read_fn`（即 `Task::read()` 的返回值）将原始数据（通常是 `String` 行）转换为 `InputItem`，然后通过一个 MPSC channel 将这些 `InputItem` 发送到处理阶段。

### 4. `Writer` Trait

`Writer` trait 定义了数据写入器的行为。框架通过 `Task::get_writer()` 方法获取具体的写入器实例。

```rust
#[async_trait::async_trait]
pub trait Writer<OutputItem: Send + 'static>: Send + Sync {
    async fn pipeline(
        &self,
        rx: tokio::sync::mpsc::Receiver<OutputItem>, // 从处理阶段接收 ProcessedItem
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
```
*   **角色**: `Writer` 从 MPSC channel 接收处理阶段生成的 `ProcessedItem`，并将它们异步写入其配置的数据目标（如文件、数据库）。

## `Task` 设计注意事项

设计一个高效且正确的 `Task` 实现时，请考虑以下几点：

1.  **`InputItem` 与 `FromRow`**：
    *   `Task` trait 要求 `InputItem` 必须实现 `for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow>`。这是为了确保如果任务配置了 SQL 数据源，框架可以从数据库行中构造出 `InputItem`。
    *   对于简单的结构体，通常可以通过 `#[derive(sqlx::FromRow)]` 轻松实现。
    *   **支持的字段类型**：与 `#[derive(sqlx::FromRow)]` 兼容的常见字段类型包括：
        *   基本类型：`bool`, `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`, `f32`, `f64`
        *   字符串：`String`
        *   字节序列：`Vec<u8>`
        *   选项：`Option<T>` (其中 `T` 是支持的类型)
        *   `sqlx` 提供的特定类型：如日期时间类型 (`chrono::NaiveDate`, `chrono::NaiveDateTime`, etc.)，`Uuid` 等 (需要启用相应 `sqlx` features)。
    *   **处理复杂类型 (如 `serde_json::Value`)**：
        *   `serde_json::Value` (或 `sqlx::types::Json<serde_json::Value>`) 不能直接与 `sqlx::any::AnyRow` 的 `FromRow` 派生兼容，因为 `sqlx` 没有为 `Json<T>` 提供通用的 `sqlx::Decode<'_, sqlx::Any>` 实现。
        *   **建议策略**：如果您的 `InputItem` 需要表示复杂结构（如 JSON 对象/数组）并且可能从 SQL 读取，请在数据库中将该列存储为 `TEXT` 类型。在您的 `InputItem` 结构体中，将对应的字段定义为 `String` 类型。`#[derive(sqlx::FromRow)]` 将能正确处理这个 `String` 字段。然后，在 `Task::process` 方法内部，您可以使用 `serde_json::from_str()` 将这个字符串解析为实际的 `serde_json::Value` 或目标结构体。
        *   **对于非 SQL 输入 (如 JSONL)**:
            *   如果您的主要输入是 JSONL，并且 `InputItem` 是一个表示整行内容的 `String` (或如 `TextLine { value: String }` 这样的简单包装器)，那么 `read` 方法会很简单。解析 JSON 字符串的逻辑会移至 `process` 方法。这种方式可以避免不必要的多次序列化/反序列化，并简化 `FromRow` 的满足。

2.  **`read` vs. `process` 的职责**：
    *   `read` 的主要职责是尽快将原始输入（通常是 `String`）转换为 `InputItem`。对于简单的行式数据，如果解析开销不大，可以在这里完成。
    *   对于复杂的解析、转换或需要外部依赖（如分词器）的逻辑，更适合放在 `process` 方法中。这使得 `InputItem` 可以保持相对简单（例如，只是一个包含原始文本的 `String` 或 `TextLine`），从而更容易满足 `FromRow` 和 `DeserializeOwned` 约束。

3.  **错误处理**：
    *   在 `read` 和 `process` 闭包中，对于不可恢复的错误，通常使用 `panic!` (如解析关键数据失败)。框架会捕获任务的 panic。
    *   对于可恢复的错误或希望跳过某个数据项的情况，`process` 方法可以返回 `None`。
    *   `Task::get_writer()` 和 `Writer::pipeline()` 返回 `Result`，允许更细致的错误传递。

4.  **状态管理**：
    *   `Task` 本身是 `&self`，但 `read` 和 `process` 返回的是 `'static` 闭包。如果这些闭包需要访问 `Task` 的状态（如配置、共享资源如连接池或分词器），通常通过 `Arc<T>` 将状态克隆并 `move` 到闭包中。
    *   对于需要在 `process` 闭包的多次调用之间共享的可变状态（如计数器），使用 `Arc<Mutex<T>>` 或 `Arc<AtomicT>`。

5.  **性能考虑**：
    *   最小化 `InputItem` 和 `ProcessedItem` 的克隆 (`.clone()`)，除非必要。
    *   在 `process` 中避免阻塞操作。如果需要执行 CPU 密集型或可能阻塞的操作，考虑使用 `tokio::task::spawn_blocking`。
    *   对于特定的Reader/Writer组件（如 `RedisReader`, `RedisWriter`）可能仍有其独立的并发参数（如`max_concurrent_tasks`），这些参数控制其组件内部的并发行为。
    *   **自动化并发管理**: 框架的默认 `Task::run` 实现内置了动态并发调整机制，可自动优化核心处理阶段的并行度，以适应不同的IO和处理负载。

## 贡献

我们欢迎社区的贡献！无论是功能增强、bug修复、文档改进还是新的 `Reader`/`Writer` 实现，请随时通过 Pull Request 或 Issue 参与进来。