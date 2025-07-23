use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use futures::future::try_join_all;
use crate::{
    BoxTask, RetryExecutor, has_cycle, DagcuterError,
    TaskResult, TaskInput
};

pub struct Dagcuter {
    tasks: HashMap<String, BoxTask>,
    results: Arc<RwLock<HashMap<String, TaskResult>>>,
    in_degrees: HashMap<String, i32>,
    dependents: HashMap<String, Vec<String>>,
    execution_order: Arc<Mutex<Vec<String>>>,
}

impl Dagcuter {
    pub fn new(tasks: HashMap<String, BoxTask>) -> Result<Self, DagcuterError> {
        if has_cycle(&tasks) {
            return Err(DagcuterError::CircularDependency);
        }

        let mut in_degrees = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        // Calculate in-degrees and dependencies
        for (name, task) in &tasks {
            in_degrees.insert(name.clone(), task.dependencies().len() as i32);

            for dep in task.dependencies() {
                dependents
                    .entry(dep)
                    .or_insert_with(Vec::new)
                    .push(name.clone());
            }
        }

        Ok(Self {
            tasks,
            results: Arc::new(RwLock::new(HashMap::new())),
            in_degrees,
            dependents,
            execution_order: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub async fn execute(
        &mut self,
        ctx: CancellationToken,
    ) -> Result<HashMap<String, TaskResult>, DagcuterError> {
        // Clear results
        self.results.write().await.clear();
        self.execution_order.lock().await.clear();

        // Create task channels
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<String>();
        let (completion_tx, mut completion_rx) = mpsc::unbounded_channel::<String>();

        // Initialize degree counters
        let in_degrees = Arc::new(Mutex::new(self.in_degrees.clone()));
        let mut remaining_tasks = self.tasks.len();

        // Send all tasks with in-degree 0 to the channel
        {
            let degrees = in_degrees.lock().await;
            for (name, &degree) in degrees.iter() {
                if degree == 0 {
                    task_tx.send(name.clone()).map_err(|_| {
                        DagcuterError::TaskExecution("Failed to send initial task".to_string())
                    })?;
                }
            }
        }

        // Start task executor
        let semaphore = Arc::new(Semaphore::new(1024)); // Limit concurrency
        let mut handles = Vec::new();

        // Task processing loop
        while remaining_tasks > 0 {
            tokio::select! {
                // Process new task
                Some(task_name) = task_rx.recv() => {
                    let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                        DagcuterError::TaskExecution("Failed to acquire semaphore".to_string())
                    })?;
                    
                    let handle = self.spawn_task(
                        ctx.clone(),
                        task_name,
                        completion_tx.clone(),
                        permit,
                    ).await;
                    handles.push(handle);
                }
                
                // Process task completion
                Some(completed_task) = completion_rx.recv() => {
                    remaining_tasks -= 1;
                    
                    // Update in-degrees of dependent tasks
                    if let Some(children) = self.dependents.get(&completed_task) {
                        let mut degrees = in_degrees.lock().await;
                        for child in children {
                            if let Some(degree) = degrees.get_mut(child) {
                                *degree -= 1;
                                if *degree == 0 {
                                    task_tx.send(child.clone()).map_err(|_| {
                                        DagcuterError::TaskExecution("Failed to send child task".to_string())
                                    })?;
                                }
                            }
                        }
                    }
                }
                
                // Check for cancellation
                _ = ctx.cancelled() => {
                    return Err(DagcuterError::ContextCancelled("Execution cancelled".to_string()));
                }
            }
        }

        // Wait for all tasks to complete
        try_join_all(handles).await.map_err(|e| {
            DagcuterError::TaskExecution(format!("Join error: {}", e))
        })?;

        // Return results
        let results = self.results.read().await.clone();
        Ok(results)
    }

    async fn spawn_task(
        &self,
        ctx: CancellationToken,
        task_name: String,
        completion_tx: mpsc::UnboundedSender<String>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> tokio::task::JoinHandle<Result<(), DagcuterError>> {
        let task = self.tasks.get(&task_name).unwrap().clone();
        let results = Arc::clone(&self.results);
        let execution_order = Arc::clone(&self.execution_order);

        tokio::spawn(async move {
            // Prepare inputs
            let inputs = Self::prepare_inputs(&task, &results).await;

            // Execute task - fixed version
            let output = Self::execute_task(ctx, &task_name, &task, inputs).await?;

            // Update execution order
            execution_order.lock().await.push(task_name.clone());

            // Store results
            results.write().await.insert(task_name.clone(), output);

            // Notify task completion
            completion_tx.send(task_name).map_err(|_| {
                DagcuterError::TaskExecution("Failed to send completion signal".to_string())
            })?;

            Ok(())
        })
    }

    async fn prepare_inputs(
        task: &BoxTask,
        results: &Arc<RwLock<HashMap<String, TaskResult>>>,
    ) -> TaskInput {
        let mut inputs = HashMap::new();
        let results_read = results.read().await;

        for dep in task.dependencies() {
            if let Some(value) = results_read.get(&dep) {
                inputs.insert(dep, serde_json::to_value(value).unwrap());
            }
        }

        inputs
    }

    // Fixed version: use new retry interface, directly return results
    async fn execute_task(
        ctx: CancellationToken,
        name: &str,
        task: &BoxTask,
        inputs: TaskInput,
    ) -> Result<TaskResult, DagcuterError> {
        let retry_executor = RetryExecutor::new(task.retry_policy());

        // Use fixed retry interface, directly return results
        retry_executor
            .execute_with_retry(ctx.clone(), name, |attempt| {
                let ctx = ctx.clone();
                let task = task.clone();
                let mut task_inputs = inputs.clone();

                async move {
                    task_inputs.insert("attempt".to_string(), serde_json::json!(attempt));

                    // PreExecution
                    task.pre_execution(ctx.clone(), &task_inputs).await?;

                    // Execute
                    let output = task.execute(ctx.clone(), &task_inputs).await?;

                    // PostExecution
                    task.post_execution(ctx, &output).await?;

                    // Directly return results, no longer using external variables
                    Ok(output)
                }
            })
            .await
    }

    pub async fn execution_order(&self) -> String {
        let order = self.execution_order.lock().await;
        let mut result = String::from("\n");

        for (i, step) in order.iter().enumerate() {
            result.push_str(&format!("{}. {}\n", i + 1, step));
        }

        result
    }

    pub fn print_graph(&self) {
        // Find all root nodes (in-degree 0)
        let mut roots = Vec::new();
        for (name, &degree) in &self.in_degrees {
            if degree == 0 {
                roots.push(name.clone());
            }
        }

        // Print from each root node
        for root in roots {
            println!("{}", root);
            self.print_chain(&root, "  ");
            println!();
        }
    }

    fn print_chain(&self, name: &str, prefix: &str) {
        if let Some(children) = self.dependents.get(name) {
            for child in children {
                println!("{}└─> {}", prefix, child);
                self.print_chain(child, &format!("{}    ", prefix));
            }
        }
    }
}

