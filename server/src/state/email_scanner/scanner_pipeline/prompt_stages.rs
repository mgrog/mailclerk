//! Stage Runner
//!
//! Handles running the three pipeline stages: main categorization (system rules),
//! user-defined categorization (user rules), and task extraction.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use sea_orm::DatabaseConnection;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use crate::{
    email::rules::{EmailRule, EmailRules, SystemEmailRules, UserEmailRules, UNKNOWN_RULE},
    model::{labels::UtilityLabels, user_email_rule::UserEmailRuleCtrl},
    observability::PipelineTracker,
    prompt::{
        mistral::{self, SYSTEM_PROMPT_TOKEN_ESTIMATE},
        task_extraction::{self, TASK_EXTRACTION_SYSTEM_PROMPT_TOKEN_ESTIMATE},
    },
    server_config::cfg,
    state::email_scanner::shared::find_matching_rule_system,
    HttpClient,
};

use super::{
    queues::PipelineQueues,
    types::{make_custom_id, CategorizationResult, FailedItem, PipelineItem, PipelineStage},
};

/// Runs batch jobs for each pipeline stage
#[derive(Clone)]
pub struct StageRunner {
    http_client: HttpClient,
    conn: DatabaseConnection,
    queues: Arc<PipelineQueues>,
    tracker: PipelineTracker,
}

impl StageRunner {
    pub fn new(
        http_client: HttpClient,
        conn: DatabaseConnection,
        queues: Arc<PipelineQueues>,
        tracker: PipelineTracker,
    ) -> Self {
        Self {
            http_client,
            conn,
            queues,
            tracker,
        }
    }

    /// Main batch processing loop - runs every batch_interval
    pub async fn run(&self, shutdown: CancellationToken) {
        let batch_interval = Duration::from_secs(cfg.scanner_pipeline.batch_interval_secs);
        let mut interval = interval(batch_interval);

        tracing::info!(
            "Stage runner started (interval: {}s)",
            cfg.scanner_pipeline.batch_interval_secs
        );

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("Stage runner shutting down");
                    break;
                }
                _ = interval.tick() => {
                    self.run_batch_cycle().await;
                }
            }
        }
    }

    /// Run one complete batch cycle: main categorization → user-defined categorization → task extraction → retry failed
    async fn run_batch_cycle(&self) {
        // 1. Requeue failed items first (from previous cycle)
        self.requeue_failed_items();

        // 2. Run main categorization pass (system rules)
        if let Err(e) = self.run_main_categorization_pass().await {
            tracing::error!("Main categorization pass batch failed: {}", e);
        }

        // 3. Run user-defined categorization pass (user rules)
        if let Err(e) = self.run_user_defined_categorization_pass().await {
            tracing::error!("User-defined categorization pass batch failed: {}", e);
        }

        // 4. Run task extraction
        if let Err(e) = self.run_task_extraction().await {
            tracing::error!("Task extraction batch failed: {}", e);
        }
    }

    /// Run main categorization pass batch (system rules, mixed users)
    async fn run_main_categorization_pass(&self) -> anyhow::Result<()> {
        let items = self.queues.drain_main_categorization_queue();
        if items.is_empty() {
            return Ok(());
        }

        let item_count = items.len();
        tracing::info!(
            "Running main categorization pass batch for {} items",
            item_count
        );

        // Calculate estimated tokens (content + categorization system prompt)
        let system_prompt_tokens = *SYSTEM_PROMPT_TOKEN_ESTIMATE as u64;
        let estimated_tokens: u64 = items
            .iter()
            .map(|i| i.estimated_content_tokens as u64 + system_prompt_tokens)
            .sum();

        // Register with tracker
        self.tracker.start_batch_job(
            PipelineStage::MainCategorization,
            item_count,
            estimated_tokens,
        );

        // Load system rules
        let system_rules = SystemEmailRules::from_db(&self.conn)
            .await
            .context("Failed to load system email rules")?;

        // Build batch requests with custom_id = "{user_id}_{email_id}"
        let system_prompt = mistral::system_prompt(system_rules.get_prompt_input());

        let requests: Vec<mistral::batch::BatchRequest> = items
            .iter()
            .map(|item| {
                let custom_id = make_custom_id(item.user_id, &item.email_id);
                let user_content = mistral::categorization_user_prompt(
                    item.simplified_message.subject.as_deref().unwrap_or(""),
                    item.simplified_message.from.as_deref().unwrap_or(""),
                    item.simplified_message.body.as_deref().unwrap_or(""),
                );

                mistral::batch::BatchRequest::for_categorization(
                    custom_id,
                    system_prompt.clone(),
                    user_content,
                )
            })
            .collect();

        // Create tracking function
        let tracker = self.tracker.clone();
        let track_fn = move |completed: u64| {
            tracker.update_batch_progress(PipelineStage::MainCategorization, completed as usize);
        };

        // Submit batch
        let batch_result = mistral::batch::run_batch_job(
            &self.http_client,
            requests,
            "main_categorization",
            Some(&track_fn),
        )
        .await;

        match batch_result {
            Ok(result) => {
                // Update tracker with job ID
                if !result.job_id.is_empty() {
                    self.tracker
                        .set_job_id(PipelineStage::MainCategorization, result.job_id.clone());
                }

                // Parse results
                let category_results = mistral::batch::parse_categorization_results(result.results);

                // Calculate actual tokens total (for logging)
                let actual_tokens: u64 =
                    category_results.iter().map(|r| r.token_usage as u64).sum();

                // Build per-email token map for accurate user quota tracking
                let token_map: HashMap<String, i64> = category_results
                    .iter()
                    .map(|r| (r.email_id.clone(), r.token_usage))
                    .collect();

                // Process results
                self.process_main_categorization_results(
                    items,
                    category_results,
                    &system_rules,
                    &token_map,
                )
                .await?;

                // Record processed count
                self.tracker.increment_processed(item_count as u64);

                // Complete the batch job
                self.tracker
                    .complete_batch_job(PipelineStage::MainCategorization, actual_tokens);

                Ok(())
            }
            Err(e) => {
                // Move all items to failed queue
                for item in items {
                    self.queues.push_to_failed_queue(FailedItem {
                        item,
                        stage: PipelineStage::MainCategorization,
                        error: e.to_string(),
                        retry_count: 0,
                    });
                }
                self.tracker
                    .fail_batch_job(PipelineStage::MainCategorization, e.to_string());
                Err(e)
            }
        }
    }

    /// Process main categorization pass results and route items to appropriate queues
    async fn process_main_categorization_results(
        &self,
        mut items: Vec<PipelineItem>,
        results: Vec<mistral::batch::CategoryResult>,
        system_rules: &SystemEmailRules,
        token_map: &HashMap<String, i64>,
    ) -> anyhow::Result<()> {
        // Build lookup map: custom_id -> result
        let results_map: HashMap<String, mistral::batch::CategoryResult> = results
            .into_iter()
            .map(|r| (r.email_id.clone(), r))
            .collect();

        // Build from map for heuristics
        let from_map: HashMap<String, String> = items
            .iter()
            .map(|i| {
                let custom_id = make_custom_id(i.user_id, &i.email_id);
                let from = i.simplified_message.from.clone().unwrap_or_default();
                (custom_id, from)
            })
            .collect();

        // Collect unique user IDs to check for user rules
        let user_ids: Vec<i32> = {
            let mut ids: Vec<i32> = items.iter().map(|i| i.user_id).collect();
            ids.sort();
            ids.dedup();
            ids
        };
        let users_with_rules = self.get_users_with_rules(&user_ids).await?;

        // Process each item
        for item in items.iter_mut() {
            let custom_id = make_custom_id(item.user_id, &item.email_id);

            if let Some(result) = results_map.get(&custom_id) {
                // Update actual tokens from API response
                if let Some(&tokens) = token_map.get(&custom_id) {
                    item.actual_tokens += tokens;
                }

                // Find matching rule with heuristics
                let (email_rule, _heuristics_used) = find_matching_rule_system(
                    result.general_category.as_deref(),
                    &result.specific_category,
                    result.confidence,
                    system_rules,
                    from_map.get(&custom_id),
                );

                // Store main categorization result
                item.first_pass_result = Some(CategorizationResult {
                    category: email_rule.mail_label.clone(),
                    ai_answer: email_rule.prompt_content.clone(),
                    ai_confidence: result.confidence,
                });

                // Route based on results
                let needs_user_defined_pass = self.should_run_user_defined_pass(
                    &email_rule,
                    users_with_rules.contains(&item.user_id),
                );
                let needs_task_extraction = email_rule.extract_tasks;

                if needs_user_defined_pass {
                    self.queues
                        .push_to_user_defined_categorization_queue(item.clone());
                } else if needs_task_extraction {
                    self.queues.push_to_task_extraction_queue(item.clone());
                } else {
                    self.queues.push_to_done_queue(item.clone());
                }
            } else {
                // No result for this item - send to failed queue
                tracing::warn!(
                    "No result for email {} (user {})",
                    item.email_id,
                    item.user_id
                );
                self.queues.push_to_failed_queue(FailedItem {
                    item: item.clone(),
                    stage: PipelineStage::MainCategorization,
                    error: "No result returned from batch".to_string(),
                    retry_count: 0,
                });
            }
        }

        Ok(())
    }

    /// Check if an item should go through user-defined categorization pass
    fn should_run_user_defined_pass(
        &self,
        main_pass_rule: &EmailRule,
        user_has_rules: bool,
    ) -> bool {
        if !user_has_rules {
            return false;
        }

        // Run user-defined pass if main categorization resulted in "Unknown"/uncategorized
        let uncategorized = UtilityLabels::Uncategorized.as_str();
        main_pass_rule.mail_label == uncategorized
    }

    /// Get user IDs that have custom email rules
    async fn get_users_with_rules(&self, user_ids: &[i32]) -> anyhow::Result<Vec<i32>> {
        let users_with_rules = UserEmailRuleCtrl::get_users_with_rules(&self.conn, user_ids)
            .await
            .context("Failed to query users with rules")?;
        Ok(users_with_rules)
    }

    /// Run user-defined categorization pass batch (user rules)
    async fn run_user_defined_categorization_pass(&self) -> anyhow::Result<()> {
        let items = self.queues.drain_user_defined_categorization_queue();
        if items.is_empty() {
            return Ok(());
        }

        let item_count = items.len();
        tracing::info!(
            "Running user-defined categorization pass batch for {} items",
            item_count
        );

        // Collect unique user IDs and load all their rules in bulk
        let user_ids: Vec<i32> = {
            let mut ids: Vec<i32> = items.iter().map(|i| i.user_id).collect();
            ids.sort();
            ids.dedup();
            ids
        };
        let user_rules_map = self.load_user_rules_bulk(&user_ids).await?;

        // Pre-compute system prompt tokens per user (since they vary by user's rule count)
        let user_system_prompt_tokens: HashMap<i32, u64> = user_rules_map
            .iter()
            .map(|(&user_id, rules)| {
                let system_prompt = mistral::system_prompt(rules.get_prompt_input());
                let tokens = tokenizer::token_count(&system_prompt).unwrap_or(0) as u64;
                (user_id, tokens)
            })
            .collect();

        // Calculate estimated tokens (content + per-user system prompt)
        let estimated_tokens: u64 = items
            .iter()
            .map(|i| {
                let system_tokens = user_system_prompt_tokens.get(&i.user_id).copied().unwrap_or(0);
                i.estimated_content_tokens as u64 + system_tokens
            })
            .sum();

        // Register with tracker
        self.tracker.start_batch_job(
            PipelineStage::UserDefinedCategorization,
            item_count,
            estimated_tokens,
        );

        // Build batch requests with per-user system prompts
        let mut requests: Vec<mistral::batch::BatchRequest> = Vec::with_capacity(items.len());

        for item in &items {
            let custom_id = make_custom_id(item.user_id, &item.email_id);

            // Get user rules or use empty rules
            let rules = user_rules_map
                .get(&item.user_id)
                .cloned()
                .unwrap_or_else(|| UserEmailRules::new(vec![], vec![]));

            let system_prompt = mistral::system_prompt(rules.get_prompt_input());
            let user_content = mistral::categorization_user_prompt(
                item.simplified_message.subject.as_deref().unwrap_or(""),
                item.simplified_message.from.as_deref().unwrap_or(""),
                item.simplified_message.body.as_deref().unwrap_or(""),
            );

            requests.push(mistral::batch::BatchRequest::for_categorization(
                custom_id,
                system_prompt,
                user_content,
            ));
        }

        // Create tracking function
        let tracker = self.tracker.clone();
        let track_fn = move |completed: u64| {
            tracker.update_batch_progress(
                PipelineStage::UserDefinedCategorization,
                completed as usize,
            );
        };

        // Submit batch
        let batch_result = mistral::batch::run_batch_job(
            &self.http_client,
            requests,
            "user_defined_categorization",
            Some(&track_fn),
        )
        .await;

        match batch_result {
            Ok(result) => {
                // Update tracker with job ID
                if !result.job_id.is_empty() {
                    self.tracker.set_job_id(
                        PipelineStage::UserDefinedCategorization,
                        result.job_id.clone(),
                    );
                }

                // Parse results
                let category_results = mistral::batch::parse_categorization_results(result.results);

                // Calculate actual tokens total (for logging)
                let actual_tokens: u64 =
                    category_results.iter().map(|r| r.token_usage as u64).sum();

                // Build per-email token map for accurate user quota tracking
                let token_map: HashMap<String, i64> = category_results
                    .iter()
                    .map(|r| (r.email_id.clone(), r.token_usage))
                    .collect();

                // Process results
                self.process_user_defined_categorization_results(
                    items,
                    category_results,
                    &user_rules_map,
                    &token_map,
                )
                .await?;

                // Record processed count
                self.tracker.increment_processed(item_count as u64);

                // Complete the batch job
                self.tracker
                    .complete_batch_job(PipelineStage::UserDefinedCategorization, actual_tokens);

                Ok(())
            }
            Err(e) => {
                // Move all items to failed queue
                for item in items {
                    self.queues.push_to_failed_queue(FailedItem {
                        item,
                        stage: PipelineStage::UserDefinedCategorization,
                        error: e.to_string(),
                        retry_count: 0,
                    });
                }
                self.tracker
                    .fail_batch_job(PipelineStage::UserDefinedCategorization, e.to_string());
                Err(e)
            }
        }
    }

    /// Load user rules in bulk using a single query with WHERE user_id IN (...)
    async fn load_user_rules_bulk(
        &self,
        user_ids: &[i32],
    ) -> anyhow::Result<HashMap<i32, UserEmailRules>> {
        use crate::email::rules::EmailRule;

        // Load all rules for all users in one query
        let all_models = UserEmailRuleCtrl::all_with_user_ids(&self.conn, user_ids.to_vec())
            .await
            .context("Failed to load user email rules")?;

        // Group by user_id
        let mut grouped: HashMap<i32, Vec<entity::user_email_rule::Model>> = HashMap::new();
        for model in all_models {
            grouped.entry(model.user_id).or_default().push(model);
        }

        // Convert to UserEmailRules
        let mut result = HashMap::new();
        for (user_id, models) in grouped {
            let rules: Vec<EmailRule> = models
                .iter()
                .map(|m| EmailRule {
                    prompt_content: m.semantic_key.clone(),
                    mail_label: m.mail_label.clone(),
                    extract_tasks: m.extract_tasks,
                    priority: m.priority,
                })
                .collect();
            result.insert(user_id, UserEmailRules::new(rules, models));
        }

        Ok(result)
    }

    /// Process user-defined categorization pass results and route items
    async fn process_user_defined_categorization_results(
        &self,
        mut items: Vec<PipelineItem>,
        results: Vec<mistral::batch::CategoryResult>,
        user_rules_map: &HashMap<i32, UserEmailRules>,
        token_map: &HashMap<String, i64>,
    ) -> anyhow::Result<()> {
        // Build lookup map: custom_id -> result
        let results_map: HashMap<String, mistral::batch::CategoryResult> = results
            .into_iter()
            .map(|r| (r.email_id.clone(), r))
            .collect();

        const USER_OVERRIDE_THRESHOLD: f32 = 0.95;

        for item in items.iter_mut() {
            let custom_id = make_custom_id(item.user_id, &item.email_id);

            if let Some(result) = results_map.get(&custom_id) {
                // Update actual tokens from API response
                if let Some(&tokens) = token_map.get(&custom_id) {
                    item.actual_tokens += tokens;
                }

                // Find matching user rule
                let rules = user_rules_map.get(&item.user_id);
                let email_rule = rules
                    .and_then(|r| {
                        r.data()
                            .iter()
                            .find(|rule| {
                                rule.prompt_content
                                    .eq_ignore_ascii_case(&result.specific_category)
                            })
                            .cloned()
                    })
                    .unwrap_or_else(|| UNKNOWN_RULE.clone());

                // Store user-defined categorization result
                item.second_pass_result = Some(CategorizationResult {
                    category: email_rule.mail_label.clone(),
                    ai_answer: email_rule.prompt_content.clone(),
                    ai_confidence: result.confidence,
                });

                // Determine final rule (user-defined pass overrides if high confidence)
                let final_rule = if result.confidence > USER_OVERRIDE_THRESHOLD {
                    &email_rule
                } else {
                    // Fall back to main categorization result
                    &*UNKNOWN_RULE
                };

                // Route based on final result
                if final_rule.extract_tasks {
                    self.queues.push_to_task_extraction_queue(item.clone());
                } else {
                    self.queues.push_to_done_queue(item.clone());
                }
            } else {
                // No result - route based on main categorization pass
                let needs_task_extraction = item
                    .first_pass_result
                    .as_ref()
                    .map(|_| true) // Default to extracting tasks if we can't determine
                    .unwrap_or(false);

                if needs_task_extraction {
                    self.queues.push_to_task_extraction_queue(item.clone());
                } else {
                    self.queues.push_to_done_queue(item.clone());
                }
            }
        }

        Ok(())
    }

    /// Run task extraction batch
    async fn run_task_extraction(&self) -> anyhow::Result<()> {
        let items = self.queues.drain_task_extraction_queue();
        if items.is_empty() {
            return Ok(());
        }

        let item_count = items.len();
        tracing::info!("Running task extraction batch for {} items", item_count);

        // Calculate estimated tokens (content + task extraction system prompt)
        let system_prompt_tokens = *TASK_EXTRACTION_SYSTEM_PROMPT_TOKEN_ESTIMATE as u64;
        let estimated_tokens: u64 = items
            .iter()
            .map(|i| i.estimated_content_tokens as u64 + system_prompt_tokens)
            .sum();

        // Register with tracker
        self.tracker
            .start_batch_job(PipelineStage::TaskExtraction, item_count, estimated_tokens);

        // Build batch requests
        let system_prompt = task_extraction::system_prompt();

        let requests: Vec<mistral::batch::BatchRequest> = items
            .iter()
            .map(|item| {
                let custom_id = make_custom_id(item.user_id, &item.email_id);
                let user_content = task_extraction::task_extraction_user_prompt(
                    item.simplified_message.subject.as_deref().unwrap_or(""),
                    item.simplified_message.body.as_deref().unwrap_or(""),
                );

                mistral::batch::BatchRequest::for_task_extraction(
                    custom_id,
                    system_prompt.clone(),
                    user_content,
                )
            })
            .collect();

        // Create tracking function
        let tracker = self.tracker.clone();
        let track_fn = move |completed: u64| {
            tracker.update_batch_progress(PipelineStage::TaskExtraction, completed as usize);
        };

        // Submit batch
        let batch_result = mistral::batch::run_batch_job(
            &self.http_client,
            requests,
            "task_extraction",
            Some(&track_fn),
        )
        .await;

        match batch_result {
            Ok(result) => {
                // Update tracker with job ID
                if !result.job_id.is_empty() {
                    self.tracker
                        .set_job_id(PipelineStage::TaskExtraction, result.job_id.clone());
                }

                // Parse results
                let task_results = mistral::batch::parse_task_extraction_results(result.results);

                // Calculate actual tokens total (for logging)
                let actual_tokens: u64 = task_results.iter().map(|r| r.token_usage as u64).sum();

                // Build per-email token map for accurate user quota tracking
                let token_map: HashMap<String, i64> = task_results
                    .iter()
                    .map(|r| (r.email_id.clone(), r.token_usage))
                    .collect();

                // Process results
                self.process_task_extraction_results(items, task_results, &token_map)?;

                // Record processed count
                self.tracker.increment_processed(item_count as u64);

                // Complete the batch job
                self.tracker
                    .complete_batch_job(PipelineStage::TaskExtraction, actual_tokens);

                Ok(())
            }
            Err(e) => {
                // Move all items to failed queue
                for item in items {
                    self.queues.push_to_failed_queue(FailedItem {
                        item,
                        stage: PipelineStage::TaskExtraction,
                        error: e.to_string(),
                        retry_count: 0,
                    });
                }
                self.tracker
                    .fail_batch_job(PipelineStage::TaskExtraction, e.to_string());
                Err(e)
            }
        }
    }

    /// Process task extraction results
    fn process_task_extraction_results(
        &self,
        mut items: Vec<PipelineItem>,
        results: Vec<mistral::batch::TaskExtractionResult>,
        token_map: &HashMap<String, i64>,
    ) -> anyhow::Result<()> {
        // Build lookup map: custom_id -> result
        let results_map: HashMap<String, mistral::batch::TaskExtractionResult> = results
            .into_iter()
            .map(|r| (r.email_id.clone(), r))
            .collect();

        for item in items.iter_mut() {
            let custom_id = make_custom_id(item.user_id, &item.email_id);

            // Update actual tokens from API response
            if let Some(&tokens) = token_map.get(&custom_id) {
                item.actual_tokens += tokens;
            }

            if let Some(result) = results_map.get(&custom_id) {
                item.extracted_tasks = result.tasks.clone();
            }

            // All items go to done queue after task extraction
            self.queues.push_to_done_queue(item.clone());
        }

        Ok(())
    }

    /// Requeue failed items for retry
    fn requeue_failed_items(&self) {
        let failed_items = self.queues.drain_failed_queue();
        if failed_items.is_empty() {
            return;
        }

        let max_retries = cfg.scanner_pipeline.max_retry_count;
        let mut requeued = 0;
        let mut dropped = 0;

        for mut failed in failed_items {
            failed.retry_count += 1;

            if failed.retry_count > max_retries {
                // Permanent failure - remove from pipeline
                tracing::warn!(
                    "Email {} (user {}) failed {} times, dropping from pipeline: {}",
                    failed.item.email_id,
                    failed.item.user_id,
                    failed.retry_count,
                    failed.error
                );
                self.queues.remove_from_pipeline(&failed.item.email_id);
                dropped += 1;
                continue;
            }

            // Requeue to appropriate stage
            match failed.stage {
                PipelineStage::MainCategorization => {
                    self.queues.push_to_main_categorization_queue(failed.item);
                }
                PipelineStage::UserDefinedCategorization => {
                    self.queues
                        .push_to_user_defined_categorization_queue(failed.item);
                }
                PipelineStage::TaskExtraction => {
                    self.queues.push_to_task_extraction_queue(failed.item);
                }
            }
            requeued += 1;
        }

        if requeued > 0 || dropped > 0 {
            tracing::info!(
                "Failed item handling: {} requeued, {} dropped (max retries exceeded)",
                requeued,
                dropped
            );
        }
    }
}
