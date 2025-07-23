use rs_dagcuter::*;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use std::sync::Arc;

// Example task implementation
struct ExampleTask {
    name: String,
    deps: Vec<String>,
}

#[async_trait]
impl Task for ExampleTask {
    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        self.deps.clone()
    }

    fn retry_policy(&self) -> Option<RetryPolicy> {
        Some(RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        })
    }

    async fn execute(
        &self,
        _ctx: CancellationToken,
        _input: &TaskInput,
    ) -> Result<TaskResult, DagcuterError> {
        println!("Executing task: {}", self.name);

        // Simulate task execution time
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut result = HashMap::new();
        result.insert("status".to_string(), serde_json::json!("completed"));
        result.insert("task_name".to_string(), serde_json::json!(self.name));
        result.insert("timestamp".to_string(), serde_json::json!(chrono::Utc::now().to_rfc3339()));
        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tasks: HashMap<String, BoxTask> = HashMap::new();

    tasks.insert("task1".to_string(), Arc::new(ExampleTask {
        name: "task1".to_string(),
        deps: vec![],
    }));

    tasks.insert("task2".to_string(), Arc::new(ExampleTask {
        name: "task2".to_string(),
        deps: vec!["task3".to_string()],
    }));

    tasks.insert("task3".to_string(), Arc::new(ExampleTask {
        name: "task3".to_string(),
        deps: vec!["task1".to_string()],
    }));

    tasks.insert("task4".to_string(), Arc::new(ExampleTask {
        name: "task4".to_string(),
        deps: vec!["task2".to_string(), "task3".to_string()],
    }));

    tasks.insert("task5".to_string(), Arc::new(ExampleTask {
        name: "task5".to_string(),
        deps: vec!["task6".to_string()],
    }));

    tasks.insert("task6".to_string(), Arc::new(ExampleTask {
        name: "task6".to_string(),
        deps: vec!["task1".to_string(), "task4".to_string(), ],
    }));



    let mut dagcuter = Dagcuter::new(tasks)?;
    let ctx = CancellationToken::new();

    println!("=== Task Dependency Graph ===");
    dagcuter.print_graph();

    println!("=== Start Executing Tasks ===");
    let start = std::time::Instant::now();
    let results = dagcuter.execute(ctx).await?;
    let duration = start.elapsed();

    println!("=== Execution Completed ===");
    println!("Execution time: {:?}", duration);
    println!("Execution results: {:#?}", results);
    println!("Execution order: {}", dagcuter.execution_order().await);

    Ok(())
}