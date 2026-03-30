// author: kodeholic (powered by Claude)
//! ScenarioEngine — 시나리오 파싱 → 봇 spawn → 타임라인 실행 → 메트릭 수집
//!
//! 설계 문서 §3.3 시나리오 엔진 동작:
//! 1. TOML 파싱
//! 2. 봇 N개 spawn, 각 봇에 개별 NetFilter 할당
//! 3. 시간축 타이머 시작
//! 4. 각 at_sec 시점에 해당 액션 실행
//! 5. 전체 봇 정지 + 메트릭 수집

use std::collections::HashMap;
use std::time::Duration;

use oxlab_bot::{Bot, BotConfig, BotStatus, ObservationsInner};
use oxlab_judge::{
    self, RecvInput, StreamInput, JudgeReport, RegressionThresholds,
    BaselineFile, CheckpointCategory, Verdict, RegressionVerdict,
    checkpoint_registry,
};
use oxlab_net::{NetFilter, NetworkProfile};
use tracing::{error, info, warn};

use crate::model::*;

/// 시나리오 실행 결과
#[derive(Debug)]
pub struct ScenarioResult {
    pub scenario_name: String,
    /// 참가자별 수신 메트릭 스냅샷
    pub recv_snapshots: HashMap<String, oxlab_bot::RecvSnapshot>,
    /// 실행 중 발생한 에러
    pub errors: Vec<String>,
    /// 판정 결과
    pub report: JudgeReport,
}

/// 시나리오 엔진
pub struct ScenarioEngine {
    scenario: Scenario,
    bots: Vec<Bot>,
    /// 봇 ID → bots 벡터 인덱스
    bot_index: HashMap<String, usize>,
    errors: Vec<String>,
    /// baseline 저장/로드 디렉토리
    baselines_dir: std::path::PathBuf,
}

impl ScenarioEngine {
    /// 시나리오 파일 로드 + 엔진 초기화
    pub fn new(scenario: Scenario) -> Self {
        Self::with_baselines_dir(scenario, std::path::PathBuf::from("baselines"))
    }

    /// baselines 디렉토리 명시 지정
    pub fn with_baselines_dir(scenario: Scenario, baselines_dir: std::path::PathBuf) -> Self {
        Self {
            scenario,
            bots: Vec::new(),
            bot_index: HashMap::new(),
            errors: Vec::new(),
            baselines_dir,
        }
    }

    /// 시나리오 실행 (진입점)
    pub async fn run(mut self) -> ScenarioResult {
        info!("═══ Scenario: {} ═══", self.scenario.meta.name);
        info!("  description: {}", self.scenario.meta.description);
        info!("  server: {}:{}", self.scenario.meta.server, self.scenario.meta.ws_port);
        info!("  room: {} ({})", self.scenario.meta.room, self.scenario.meta.mode);
        info!("  participants: {}", self.scenario.participants.len());
        info!("  actions: {}", self.scenario.actions.len());

        // 봇 사전 생성 (시그널링 미접속 상태)
        self.create_bots();

        // 액션 정렬 (at_sec 순)
        let mut actions: Vec<Action> = std::mem::take(&mut self.scenario.actions);
        actions.sort_by(|a, b| a.at_sec.partial_cmp(&b.at_sec).unwrap());

        // 타임라인 실행
        let start = tokio::time::Instant::now();

        for action in &actions {
            // at_sec까지 대기
            let target = Duration::from_secs_f64(action.at_sec);
            let elapsed = start.elapsed();
            if target > elapsed {
                let wait = target - elapsed;
                // 대기 중 heartbeat (5초마다)
                self.wait_with_heartbeat(wait).await;
            }

            info!("── @{:.1}s {:?} ──", action.at_sec, action.action_type);
            self.execute_action(action).await;
        }

        // 최종 메트릭 수집
        info!("═══ Collecting metrics ═══");
        let mut recv_snapshots = HashMap::new();
        for (id, &idx) in &self.bot_index {
            let bot = &self.bots[idx];
            if bot.status == BotStatus::Publishing {
                let snap = bot.recv_snapshot();
                bot.log_recv_metrics();
                recv_snapshots.insert(id.clone(), snap);
            }
        }

        // 관측 데이터 수집 (L1 체크포인트용)
        let mut bot_observations: HashMap<String, ObservationsInner> = HashMap::new();
        for (id, &idx) in &self.bot_index {
            let obs = self.bots[idx].observations.snapshot();
            bot_observations.insert(id.clone(), obs);
        }

        // 전체 봇 종료
        info!("═══ Disconnecting ═══");
        for bot in &mut self.bots {
            bot.disconnect().await;
        }

        // 판정 실행
        info!("═══ Judging ═══");
        let regression_thresholds = match &self.scenario.meta.judgement {
            Some(name) => RegressionThresholds::resolve(name),
            None => RegressionThresholds::default(),
        };

        let judge_inputs: HashMap<String, RecvInput> = recv_snapshots.iter()
            .map(|(id, snap)| {
                let input = RecvInput {
                    total_packets: snap.total_packets,
                    total_lost: snap.total_lost,
                    total_ooo: snap.total_ooo,
                    streams: snap.streams.iter().map(|s| StreamInput {
                        ssrc: s.ssrc,
                        pt: s.pt,
                        packets_received: s.packets_received,
                        packets_lost: s.packets_lost,
                        out_of_order: s.out_of_order,
                        jitter: s.jitter,
                    }).collect(),
                };
                (id.clone(), input)
            })
            .collect();

        // Layer 1: 카테고리 필터링
        let categories: Vec<CheckpointCategory> = if self.scenario.meta.categories.is_empty() {
            // 비어있으면 전체 적용
            vec![]
        } else {
            self.scenario.meta.categories.iter()
                .filter_map(|s| CheckpointCategory::from_str_loose(s))
                .collect()
        };

        let applicable_defs = if categories.is_empty() {
            checkpoint_registry::REGISTRY.iter().collect::<Vec<_>>()
        } else {
            checkpoint_registry::find_by_categories(&categories)
        };
        let total_applicable = applicable_defs.len();

        // 프로파일명 추출 (첫 번째 참가자 기준)
        let profile_name = self.scenario.participants.first()
            .map(|p| p.profile.as_str())
            .unwrap_or("unknown");

        // Layer 1 체크포인트 평가
        let layer1_checkpoints = crate::checkpoint_eval::evaluate(
            &recv_snapshots,
            &bot_observations,
            &categories,
            &self.scenario.meta.mode,
            profile_name,
        );

        // Layer 2: baseline 로드
        let baseline = BaselineFile::try_load(
            &self.baselines_dir,
            &self.scenario.meta.name,
            profile_name,
        );

        let report = oxlab_judge::judge(
            &self.scenario.meta.name,
            profile_name,
            layer1_checkpoints,
            total_applicable,
            &judge_inputs,
            baseline.as_ref(),
            &regression_thresholds,
            &self.errors,
        );

        report.print_summary();

        // baseline 갱신: L1 PASS 이고 L2 REGRESS 아닐 때만
        let should_update = report.layer1.verdict == Verdict::Pass
            && report.layer2.verdict != RegressionVerdict::Regressed;
        if should_update {
            let new_baseline = oxlab_judge::create_baseline(&report);
            let baseline_path = BaselineFile::resolve_path(
                &self.baselines_dir,
                &self.scenario.meta.name,
                profile_name,
            );
            if let Err(e) = std::fs::create_dir_all(&self.baselines_dir) {
                error!("failed to create baselines dir: {}", e);
            } else if let Err(e) = new_baseline.save(&baseline_path) {
                error!("failed to save baseline: {}", e);
            } else {
                info!("baseline saved: {}", baseline_path.display());
            }
        } else {
            info!("baseline NOT updated (L1={} L2={})",
                report.layer1.verdict, report.layer2.verdict);
        }

        info!("═══ Scenario '{}' complete (errors={}) ═══",
            self.scenario.meta.name, self.errors.len());

        ScenarioResult {
            scenario_name: self.scenario.meta.name.clone(),
            recv_snapshots,
            errors: self.errors,
            report,
        }
    }

    // ── 봇 생성 ──

    fn create_bots(&mut self) {
        for (i, pdef) in self.scenario.participants.iter().enumerate() {
            let profile = NetworkProfile::builtin(&pdef.profile)
                .unwrap_or_else(|| {
                    warn!("unknown profile '{}', using pristine", pdef.profile);
                    NetworkProfile::pristine()
                });

            let config = BotConfig {
                id: pdef.id.clone(),
                server: self.scenario.meta.server.clone(),
                ws_port: self.scenario.meta.ws_port,
                room_name: self.scenario.meta.room.clone(),
                mode: self.scenario.meta.mode.clone(),
                profile: Some(pdef.profile.clone()),
            };

            let net_filter = NetFilter::new(profile.conditions);
            let bot = Bot::new(config, Some(net_filter));

            self.bot_index.insert(pdef.id.clone(), i);
            self.bots.push(bot);
        }
        info!("created {} bots", self.bots.len());
    }

    // ── 액션 실행 ──

    async fn execute_action(&mut self, action: &Action) {
        match action.action_type {
            ActionType::AllJoin => self.action_all_join().await,
            ActionType::StartMedia => self.action_start_media().await,
            ActionType::PttRequest => self.action_ptt_request(action).await,
            ActionType::PttRelease => self.action_ptt_release(action).await,
            ActionType::PttAlternate => self.action_ptt_alternate(action).await,
            ActionType::NetworkTransition => self.action_network_transition(action).await,
            ActionType::KillBot => self.action_kill_bot(action).await,
            ActionType::Wait => self.action_wait(action).await,
        }
    }

    /// 전체 봇 접속 + 방 입장
    async fn action_all_join(&mut self) {
        let mut room_id: Option<String> = None;

        for i in 0..self.bots.len() {
            let bot_id = self.bots[i].id().to_string();
            let result = if i == 0 {
                self.bots[i].connect_and_join().await
            } else {
                let rid = match &room_id {
                    Some(r) => r.clone(),
                    None => {
                        self.errors.push(format!("{}: no room_id", bot_id));
                        continue;
                    }
                };
                self.bots[i].join_existing_room(&rid).await
            };

            match result {
                Ok(()) => {
                    if i == 0 {
                        room_id = self.bots[i].room_id.clone();
                        info!("[{}] room created: {:?}", bot_id, room_id);
                    }
                }
                Err(e) => {
                    error!("[{}] join failed: {}", bot_id, e);
                    self.errors.push(format!("{}: join failed: {}", bot_id, e));
                }
            }
        }

        let joined = self.bots.iter().filter(|b| b.status == BotStatus::Joined).count();
        info!("all_join: {}/{} bots joined", joined, self.bots.len());
    }

    /// 전체 봇 미디어 셋업 + publishing 시작
    async fn action_start_media(&mut self) {
        for i in 0..self.bots.len() {
            let bot_id = self.bots[i].id().to_string();
            if self.bots[i].status != BotStatus::Joined { continue; }

            if let Err(e) = self.bots[i].setup_media().await {
                error!("[{}] media setup failed: {}", bot_id, e);
                self.errors.push(format!("{}: media setup: {}", bot_id, e));
                continue;
            }
            if let Err(e) = self.bots[i].publish_intent().await {
                error!("[{}] publish_intent failed: {}", bot_id, e);
                self.errors.push(format!("{}: publish_intent: {}", bot_id, e));
                continue;
            }
            if let Err(e) = self.bots[i].start_publishing() {
                error!("[{}] start_publishing failed: {}", bot_id, e);
                self.errors.push(format!("{}: start_publishing: {}", bot_id, e));
                continue;
            }
        }

        let publishing = self.bots.iter().filter(|b| b.status == BotStatus::Publishing).count();
        info!("start_media: {}/{} bots publishing", publishing, self.bots.len());

        // stream discovery 대기 + TRACKS_ACK
        tokio::time::sleep(Duration::from_secs(2)).await;
        for bot in &mut self.bots {
            if bot.status == BotStatus::Publishing {
                if let Err(e) = bot.process_events().await {
                    error!("[{}] process_events failed: {}", bot.id(), e);
                }
            }
        }
    }

    /// PTT 발화권 요청
    async fn action_ptt_request(&mut self, action: &Action) {
        let actor = match &action.actor {
            Some(a) => a.clone(),
            None => {
                error!("ptt_request: missing actor");
                self.errors.push("ptt_request: missing actor".into());
                return;
            }
        };
        let priority = action.priority.unwrap_or(0);

        if let Some(&idx) = self.bot_index.get(&actor) {
            match self.bots[idx].floor_request_ws(priority).await {
                Ok(granted) => info!("[{}] ptt_request granted={}", actor, granted),
                Err(e) => {
                    error!("[{}] ptt_request failed: {}", actor, e);
                    self.errors.push(format!("{}: ptt_request: {}", actor, e));
                }
            }
        } else {
            error!("ptt_request: unknown actor '{}'", actor);
        }
    }

    /// PTT 발화권 해제
    async fn action_ptt_release(&mut self, action: &Action) {
        let actor = match &action.actor {
            Some(a) => a.clone(),
            None => {
                error!("ptt_release: missing actor");
                self.errors.push("ptt_release: missing actor".into());
                return;
            }
        };

        if let Some(&idx) = self.bot_index.get(&actor) {
            if let Err(e) = self.bots[idx].floor_release_ws().await {
                error!("[{}] ptt_release failed: {}", actor, e);
                self.errors.push(format!("{}: ptt_release: {}", actor, e));
            }
        } else {
            error!("ptt_release: unknown actor '{}'", actor);
        }
    }

    /// 복수 봇 교대 발화
    async fn action_ptt_alternate(&mut self, action: &Action) {
        let actors = match &action.actors {
            Some(a) if !a.is_empty() => a.clone(),
            _ => {
                error!("ptt_alternate: missing or empty actors");
                self.errors.push("ptt_alternate: missing actors".into());
                return;
            }
        };
        let count = action.count.unwrap_or(1) as usize;
        let interval = Duration::from_secs_f64(action.interval_sec.unwrap_or(3.0));

        info!("ptt_alternate: actors={:?} count={} interval={:.1}s",
            actors, count, interval.as_secs_f64());

        for round in 0..count {
            for actor_id in &actors {
                let idx = match self.bot_index.get(actor_id) {
                    Some(&i) => i,
                    None => {
                        error!("ptt_alternate: unknown actor '{}'", actor_id);
                        continue;
                    }
                };

                // request
                match self.bots[idx].floor_request_ws(0).await {
                    Ok(granted) => {
                        if granted {
                            info!("[{}] round {}/{} talking {:.1}s",
                                actor_id, round + 1, count, interval.as_secs_f64());
                            // talk duration
                            self.wait_with_heartbeat(interval).await;
                            // release
                            if let Err(e) = self.bots[idx].floor_release_ws().await {
                                error!("[{}] ptt_release failed: {}", actor_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("[{}] ptt_request failed: {}", actor_id, e);
                    }
                }

                // 화자 전환 간 짧은 갭
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    /// 네트워크 프로파일 동적 전환
    async fn action_network_transition(&mut self, action: &Action) {
        let actor = match &action.actor {
            Some(a) => a.clone(),
            None => {
                error!("network_transition: missing actor");
                return;
            }
        };
        let profile_name = match &action.profile {
            Some(p) => p.clone(),
            None => {
                error!("network_transition: missing profile");
                return;
            }
        };

        let profile = NetworkProfile::builtin(&profile_name)
            .unwrap_or_else(|| {
                warn!("unknown profile '{}', using pristine", profile_name);
                NetworkProfile::pristine()
            });

        if let Some(&idx) = self.bot_index.get(&actor) {
            self.bots[idx].update_net_filter(profile.conditions.clone());
            info!("[{}] network -> {} (loss={:.1}% delay={}ms)",
                actor, profile_name,
                profile.conditions.loss_percent, profile.conditions.delay_ms);
        } else {
            error!("network_transition: unknown actor '{}'", actor);
        }
    }

    /// 봇 강제 종료 (좀비 시뮬레이션)
    async fn action_kill_bot(&mut self, action: &Action) {
        let actor = match &action.actor {
            Some(a) => a.clone(),
            None => {
                error!("kill_bot: missing actor");
                return;
            }
        };

        if let Some(&idx) = self.bot_index.get(&actor) {
            self.bots[idx].disconnect().await;
            info!("[{}] killed (zombie simulation)", actor);
        } else {
            error!("kill_bot: unknown actor '{}'", actor);
        }
    }

    /// 대기 (heartbeat + process_events 포함)
    async fn action_wait(&mut self, action: &Action) {
        let secs = action.duration_sec.unwrap_or(5.0);
        info!("wait {:.1}s", secs);
        self.wait_with_heartbeat(Duration::from_secs_f64(secs)).await;
    }

    // ── heartbeat 포함 대기 ──

    async fn wait_with_heartbeat(&mut self, duration: Duration) {
        let deadline = tokio::time::Instant::now() + duration;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { break; }

            let sleep_dur = remaining.min(Duration::from_secs(5));
            tokio::time::sleep(sleep_dur).await;

            for bot in &mut self.bots {
                let active = bot.status == BotStatus::Joined
                    || bot.status == BotStatus::MediaReady
                    || bot.status == BotStatus::Publishing;
                if active {
                    let _ = bot.heartbeat().await;
                }
                if bot.status == BotStatus::Publishing {
                    let _ = bot.process_events().await;
                }
            }

            if tokio::time::Instant::now() >= deadline { break; }
        }
    }
}
