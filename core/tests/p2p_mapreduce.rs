use std::collections::HashMap;
use std::time::Duration;

use libp2p::futures::StreamExt;
use libp2p::gossipsub;
use libp2p::swarm::SwarmEvent;
use nitro_core::network::{
    create_swarm_with_mdns, process_swarm_maintenance, publish_result_payload,
    publish_task_payload, ResultPayload, TaskOutcome, TaskPayload, TASK_ALLOC_TOPIC,
    TASK_RESULTS_TOPIC,
};
use nitro_core::runtime::{NitroRuntime, SandboxTrapError, DEFAULT_MAX_MEMORY_BYTES};

const WORD_COUNT_WAT: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "map_line") (param $ptr i32) (param $len i32) (result i32)
            (local $i i32)
            (local $in_word i32)
            (local $count i32)
            (local $ch i32)
            (local.set $i (i32.const 0))
            (local.set $in_word (i32.const 0))
            (local.set $count (i32.const 0))
            (block $break
                (loop $loop
                    (br_if $break (i32.ge_u (local.get $i) (local.get $len)))
                    (local.set $ch (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
                    (if (i32.or
                            (i32.eq (local.get $ch) (i32.const 32))
                            (i32.or (i32.eq (local.get $ch) (i32.const 10)) (i32.eq (local.get $ch) (i32.const 9))))
                        (then
                            (local.set $in_word (i32.const 0))
                        )
                        (else
                            (if (i32.eqz (local.get $in_word))
                                (then
                                    (local.set $in_word (i32.const 1))
                                    (local.set $count (i32.add (local.get $count) (i32.const 1)))
                                )
                            )
                        )
                    )
                    (local.set $i (i32.add (local.get $i) (i32.const 1)))
                    (br $loop)
                )
            )
            (local.get $count)
        )
    )
"#;

const INFINITE_LOOP_WAT: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "map_line") (param i32 i32) (result i32)
            (loop $inf
                (br $inf)
            )
            (i32.const 0)
        )
    )
"#;

#[tokio::test]
async fn end_to_end_p2p_mapreduce_and_fuel_trap_roundtrip() {
    let valid_wasm = wat::parse_str(WORD_COUNT_WAT).expect("parse valid WAT");
    let evil_wasm = wat::parse_str(INFINITE_LOOP_WAT).expect("parse infinite loop WAT");

    let mut worker_swarm = create_swarm_with_mdns(false).await.expect("worker swarm");
    let mut coord_swarm = create_swarm_with_mdns(false)
        .await
        .expect("coordinator swarm");

    let worker_peer_id = worker_swarm.local_peer_id().to_string();

    worker_swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().expect("valid addr"))
        .expect("worker listen");
    coord_swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().expect("valid addr"))
        .expect("coord listen");

    // Discover worker's ephemeral TCP listen address.
    let worker_addr = loop {
        if let SwarmEvent::NewListenAddr { address, .. } = worker_swarm.select_next_some().await {
            break address;
        }
    };
    coord_swarm.dial(worker_addr).expect("coord dials worker");

    let alloc_topic_hash = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC).hash();
    let results_topic_hash = gossipsub::IdentTopic::new(TASK_RESULTS_TOPIC).hash();

    let mut worker_subscribed_to_alloc = false;
    let mut coord_subscribed_to_results = false;
    let mut dispatched = false;

    let runtime = NitroRuntime::new();
    let mut received_results: HashMap<String, ResultPayload> = HashMap::new();

    let test_timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(test_timeout);

    loop {
        tokio::select! {
            _ = &mut test_timeout => {
                panic!(
                    "P2P integration test timed out. alloc_sub={worker_subscribed_to_alloc}, res_sub={coord_subscribed_to_results}, results={:?}",
                    received_results.keys().collect::<Vec<_>>()
                );
            }
            event = worker_swarm.select_next_some() => {
                if let Some(gs_event) = process_swarm_maintenance(&mut worker_swarm, event) {
                    match gs_event {
                        gossipsub::Event::Subscribed { topic, .. } => {
                            if topic == results_topic_hash {
                                coord_subscribed_to_results = true;
                            }
                        }
                        gossipsub::Event::Message { message, .. } if message.topic == alloc_topic_hash => {
                            let task = TaskPayload::from_bytes(&message.data).expect("decode task");
                            task.verify_wasm_integrity().expect("integrity check");

                            let outcome = match runtime.execute_with_options(
                                &task.wasm_binary,
                                &task.data_chunk,
                                task.fuel,
                                DEFAULT_MAX_MEMORY_BYTES,
                                &task.entry_fn,
                            ) {
                                Ok(metrics) => ResultPayload::from_metrics(
                                    task.task_id,
                                    task.job_id,
                                    worker_peer_id.clone(),
                                    task.attempt,
                                    &metrics,
                                ),
                                Err(err) => {
                                    let status = match err.downcast_ref::<SandboxTrapError>() {
                                        Some(SandboxTrapError::OutOfFuel { .. }) => TaskOutcome::SecurityTrap {
                                            kind: "OutOfFuel".to_string(),
                                            reason: err.to_string(),
                                        },
                                        Some(SandboxTrapError::MemoryLimitExceeded { .. }) => TaskOutcome::SecurityTrap {
                                            kind: "MemoryLimitExceeded".to_string(),
                                            reason: err.to_string(),
                                        },
                                        _ => TaskOutcome::ExecutionError {
                                            reason: err.to_string(),
                                        },
                                    };
                                    ResultPayload::failure(
                                        task.task_id,
                                        task.job_id,
                                        worker_peer_id.clone(),
                                        task.attempt,
                                        status,
                                    )
                                }
                            };
                            publish_result_payload(&mut worker_swarm, &outcome)
                                .expect("publish result");
                        }
                        _ => {}
                    }
                }
            }
            event = coord_swarm.select_next_some() => {
                if let Some(gs_event) = process_swarm_maintenance(&mut coord_swarm, event) {
                    match gs_event {
                        gossipsub::Event::Subscribed { topic, .. } => {
                            if topic == alloc_topic_hash {
                                worker_subscribed_to_alloc = true;
                            }
                        }
                        gossipsub::Event::Message { message, .. } if message.topic == results_topic_hash => {
                            let result = ResultPayload::from_bytes(&message.data).expect("decode result");
                            received_results.insert(result.task_id.clone(), result);
                            if received_results.len() >= 2 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if !dispatched && worker_subscribed_to_alloc && coord_subscribed_to_results {
            let valid_task = TaskPayload::new(
                "task-valid",
                valid_wasm.clone(),
                "nitro edge compute rust wasm",
                1_000_000,
            );
            let evil_task = TaskPayload::new("task-evil", evil_wasm.clone(), "trigger loop", 2_000);

            publish_task_payload(&mut coord_swarm, &valid_task).expect("publish valid task");
            publish_task_payload(&mut coord_swarm, &evil_task).expect("publish evil task");
            dispatched = true;
        }
    }

    let ok_res = received_results
        .get("task-valid")
        .expect("valid task result present");
    assert_eq!(ok_res.status, TaskOutcome::Success);
    assert_eq!(ok_res.computed_value, 5);
    assert!(ok_res.fuel_consumed > 0);

    let trap_res = received_results
        .get("task-evil")
        .expect("trapped task result present");
    assert!(matches!(
        trap_res.status,
        TaskOutcome::SecurityTrap { ref kind, .. } if kind == "OutOfFuel"
    ));
}
