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
    // 定义任务输入的数据单元类型（通常为结构体或枚举）
    type ReadItem: Send + Sync + 'static + Debug + Clone + DeserializeOwned + Unpin
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
    async fn process(&self, item: Self::ReadItem) -> Result<Option<Self::ProcessedItem>, FrameworkError>;
    // 根据配置获取数据写入器实例
    async fn get_writer(&self, endpoint_config: &DataEndpoint)
        -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError>;
    // 任务执行的入口点（框架已提供默认实现，支持多输入端点自动合并）
    async fn run(&self) -> Result<(), FrameworkError>;
}
```

### 2. `InputItem` 与多类型支持

`InputItem` 是一个枚举类型，支持多种输入数据结构，便于不同Reader/数据源的统一流式处理。例如：

```rust
#[derive(Debug)]
pub enum InputItem {
    String(String),
    FastaItem(FastaItem),
    // 可扩展更多类型
}
```
- 各Reader会将原始数据包装为对应的`InputItem`变体。
- `Task::read`闭包负责将`InputItem`转换为具体的`ReadItem`。

### 3. 多输入端点自动合并机制

- 支持`get_inputs_info()`返回多个`DataEndpoint`（如多个FASTA文件、多个数据库表等）。
- 框架会为每个输入端点自动创建Reader，所有Reader的数据通过MPSC通道合并到主broker。
- 后续`process`和`writer`阶段对所有输入数据统一处理，无需关心数据来源。
- 适用于多文件批量处理、数据聚合等场景。

### 4. `Reader` Trait

```rust
#[async_trait::async_trait]
pub trait Reader<Item>: Send + Sync
where
    Item: Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
    ) -> tokio::sync::mpsc::Receiver<Item>;
}
```
- 每个Reader独立读取数据，自动并发。
- 支持多种数据源类型。

### 5. `Writer` Trait

```rust
#[async_trait::async_trait]
pub trait Writer<OutputItem: Send + 'static>: Send + Sync {
    async fn pipeline(
        &self,
        rx: tokio::sync::mpsc::Receiver<OutputItem>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
```

### 6. main.rs任务调度说明

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

---

## 贡献

我们欢迎社区的贡献！无论是功能增强、bug修复、文档改进还是新的 `Reader`/`Writer` 实现，请随时通过 Pull Request 或 Issue 参与进来。