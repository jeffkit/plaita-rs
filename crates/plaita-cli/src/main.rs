//! plaita-rs CLI: load, validate and run plaita deployment flows.

use std::io::Read;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use serde_json::Value;

mod ns;

#[derive(Parser)]
#[command(
    name = "plaita-rs",
    version,
    about = "plaita flow engine in Rust — parse and run plaita deployment flows"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Load and validate a flow document (normalization + structural rules).
    Validate {
        /// Path to the flow JSON (deployment form).
        flow: String,
    },
    /// Execute a flow and print the JSON result.
    Run {
        /// Path to the flow JSON (deployment form).
        flow: String,
        /// Input JSON (inline). Defaults to {}.
        #[arg(long)]
        input: Option<String>,
        /// Read the input JSON from a file instead.
        #[arg(long)]
        input_file: Option<String>,
        /// Print flow/node lifecycle events as NDJSON to stderr.
        #[arg(long)]
        events: bool,
        /// Pin the distributed execution id ($EXECUTION_ID).
        #[arg(long)]
        execution_id: Option<String>,
    },
    /// Evaluate one expression against a context (handy for debugging).
    Eval {
        /// The expression: "$F.add(1, 2)", "$INPUT.x", "text {%$INPUT%}"...
        expr: String,
        /// Context JSON: keys are roots without the $ ("INPUT", "NODE", ...).
        #[arg(long)]
        context: Option<String>,
    },
    /// NS-M4 nested-suspend differential runner (hidden): run one case file
    /// in-process (execute → suspend → resume…) and print the unified result
    /// JSON. Driven by tools/nested_suspend_diff/diffcheck.py.
    #[command(hide = true)]
    Ns {
        /// Path to the case JSON.
        case: String,
    },
}

fn read_flow(path: &str) -> Result<Vec<u8>, String> {
    if path == "-" {
        let mut buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buf)
            .map_err(|e| format!("read stdin: {e}"))?;
        return Ok(buf);
    }
    std::fs::read(path).map_err(|e| format!("read {path}: {e}"))
}

fn parse_input(cli_input: Option<String>, file: Option<String>) -> Result<Value, String> {
    let text = match (cli_input, file) {
        (Some(_), Some(_)) => return Err("--input and --input-file are mutually exclusive".into()),
        (Some(s), None) => s,
        (None, Some(f)) => std::fs::read_to_string(&f).map_err(|e| format!("read {f}: {e}"))?,
        (None, None) => "{}".to_string(),
    };
    serde_json::from_str(&text).map_err(|e| format!("parse input: {e}"))
}

fn main() {
    let cli = Cli::parse();
    let code = match run(cli) {
        Ok(()) => 0,
        Err(msg) => {
            eprintln!("error: {msg}");
            1
        }
    };
    std::process::exit(code);
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Validate { flow } => {
            let data = read_flow(&flow)?;
            let loaded = plaita_engine::ir::load(&data).map_err(|e| e.to_string())?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "flow_id": loaded.id,
                    "nodes": loaded.nodes.len(),
                })
            );
            Ok(())
        }
        Command::Run {
            flow,
            input,
            input_file,
            events,
            execution_id,
        } => {
            let data = read_flow(&flow)?;
            let loaded = plaita_engine::ir::load(&data).map_err(|e| e.to_string())?;
            let args = parse_input(input, input_file)?;
            let mut host = plaita_engine::Host::new();
            if let Some(eid) = execution_id {
                host = plaita_engine::Host::with_execution_id(eid);
            }
            if events {
                let flow_label = loaded.id.clone();
                host = host.listen(Arc::new(move |event: &plaita_engine::Event| {
                    println_event(&flow_label, event);
                }));
            }
            let outcome = loaded.execute(&args, &host);
            // 后台分支留有副作用（写文件/通知）时不在途丢弃
            host.join_background();
            match outcome {
                Ok(result) => {
                    println!(
                        "{}",
                        serde_json::to_string(&result)
                            .map_err(|e| format!("render result: {e}"))?
                    );
                    Ok(())
                }
                Err(e) => Err(format!("{e}")),
            }
        }
        Command::Eval { expr, context } => {
            let mut roots = plaita_expression::MapRoots::new();
            if let Some(ctx) = context {
                let parsed: Value =
                    serde_json::from_str(&ctx).map_err(|e| format!("parse context: {e}"))?;
                let Value::Object(map) = parsed else {
                    return Err("context must be a JSON object".into());
                };
                for (k, v) in map {
                    roots.set(k.trim_start_matches('$'), v);
                }
            }
            let out = plaita_expression::eval_value(&Value::String(expr.clone()), &roots)
                .map_err(|e| e.to_string())?;
            println!(
                "{}",
                serde_json::to_string(&out).map_err(|e| e.to_string())?
            );
            Ok(())
        }
        Command::Ns { case } => ns::run(&case),
    }
}

fn println_event(flow_label: &str, event: &plaita_engine::Event) {
    // "flow" 必须取事件自带的 flow_id：子流/分支事件的归属在引擎侧已经
    // 算对，用根标签顶替会把子流故障记到根上（假红）或让子流在观测面上
    // 凭空消失（假绿）。这次运行的根 flow 另以 "root" 给出。
    let line = match event {
        plaita_engine::Event::FlowStart { flow_id } => serde_json::json!({
            "event": "flow_start", "flow": flow_id, "root": flow_label,
        }),
        plaita_engine::Event::FlowEnd {
            flow_id,
            result,
            error,
            // 根执行终态快照是 worker 层的持久化面，CLI 观测输出不展开
            //（展开会污染对拍 JSON 的形状）。
            context: _,
        } => serde_json::json!({
            "event": "flow_end", "flow": flow_id, "root": flow_label,
            "result": result, "error": error,
        }),
        plaita_engine::Event::NodeStart {
            flow_id,
            node_id,
            node_type,
        } => serde_json::json!({
            "event": "node_start", "flow": flow_id, "root": flow_label,
            "node": node_id, "type": node_type,
        }),
        plaita_engine::Event::NodeEnd {
            flow_id,
            node_id,
            node_type,
            result,
            error,
        } => {
            serde_json::json!({
                "event": "node_end", "flow": flow_id, "root": flow_label,
                "node": node_id, "type": node_type,
                "result": result, "error": error,
            })
        }
    };
    eprintln!("{}", line);
}
