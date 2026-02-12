use clap::Parser;
use glob::glob;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Parser, Debug)]
#[command(author, version, about = "Batch globbed inputs into submit jobs")]
pub struct Cli {
    /// Path to the shell script to execute for each input file.
    #[arg(long)]
    script: PathBuf,

    /// One or more glob patterns or literal input tokens.
    #[arg(long, num_args = 1..)]
    glob: Vec<String>,

    /// Either a named flag (e.g. --input), a positional marker like $2,
    /// or a template that contains $1 placeholders.
    #[arg(long, default_value = "--input")]
    input_flag: String,

    /// Number of output batch scripts/jobs to create.
    #[arg(long, default_value_t = 1)]
    batch: usize,

    /// Directory where generated batch scripts are stored.
    #[arg(long, default_value = ".batchelor")]
    out_dir: PathBuf,

    /// Submission command, e.g. "sbatch --mem=50G --mincpus 1" or "bash".
    #[arg(long, default_value = "sbatch")]
    submit: String,

    /// Prefix for generated job names.
    #[arg(long, default_value = "batch")]
    job_name_prefix: String,

    /// Additional args passed to your script for each invocation.
    #[arg(long, num_args = 1.., trailing_var_arg = true)]
    script_args: Vec<String>,

    /// Print what would be submitted without running the submit command.
    #[arg(long)]
    dry_run: bool,

    /// Keep generated intermediate batch scripts after successful submission.
    #[arg(long)]
    keep: bool,
}

pub fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    if cli.batch == 0 {
        return Err("--batch must be >= 1".into());
    }

    if !cli.script.exists() {
        return Err(format!("script does not exist: {}", cli.script.display()).into());
    }

    let script_abs = fs::canonicalize(&cli.script)?;
    let mut inputs = expand_inputs(&cli.glob)?;

    if inputs.is_empty() {
        return Err(format!("no inputs matched from --glob {:?}", cli.glob).into());
    }

    inputs.sort();
    fs::create_dir_all(&cli.out_dir)?;
    cleanup_old_batch_scripts(&cli.out_dir, &cli.job_name_prefix)?;

    let batch_count = cli.batch.min(inputs.len());
    println!(
        "Found {} input files. Creating {} job(s).",
        inputs.len(),
        batch_count
    );

    let groups = split_evenly(&inputs, batch_count);
    for (idx, chunk) in groups.iter().enumerate() {
        let batch_idx = idx + 1;
        let job_name = format!("{}-{:04}", cli.job_name_prefix, batch_idx);
        let job_script_path = cli.out_dir.join(format!("{}.batch.sh", job_name));

        write_job_script(
            &job_script_path,
            &script_abs,
            &cli.input_flag,
            chunk,
            &cli.script_args,
        )?;

        if cli.dry_run {
            println!(
                "[dry-run] {} {}",
                cli.submit,
                shell_quote_path(&job_script_path)
            );
        } else {
            submit_job(&cli.submit, &job_script_path)?;
            if !cli.keep {
                fs::remove_file(&job_script_path)?;
            }
        }
    }

    Ok(())
}

fn split_evenly<T>(items: &[T], groups: usize) -> Vec<&[T]> {
    let mut out = Vec::new();
    let base = items.len() / groups;
    let remainder = items.len() % groups;
    let mut start = 0usize;
    for i in 0..groups {
        let size = if i < remainder { base + 1 } else { base };
        let end = start + size;
        out.push(&items[start..end]);
        start = end;
    }
    out
}

fn cleanup_old_batch_scripts(out_dir: &Path, job_name_prefix: &str) -> io::Result<()> {
    let prefix = format!("{}-", job_name_prefix);
    for entry in fs::read_dir(out_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with(&prefix) && name.ends_with(".batch.sh") {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn expand_inputs(patterns: &[String]) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();

    for pattern in patterns {
        if has_glob_meta(pattern) {
            for entry in glob(pattern)? {
                match entry {
                    Ok(path) => {
                        let normalized = if path.exists() {
                            fs::canonicalize(path)?
                                .to_string_lossy()
                                .into_owned()
                        } else {
                            path.to_string_lossy().into_owned()
                        };
                        out.push(normalized);
                    }
                    Err(e) => return Err(Box::new(e)),
                }
            }
        } else {
            let path = Path::new(pattern);
            if path.exists() {
                out.push(fs::canonicalize(path)?.to_string_lossy().into_owned());
            } else {
                out.push(pattern.to_string());
            }
        }
    }

    Ok(out)
}

fn write_job_script(
    output_path: &Path,
    script: &Path,
    input_flag: &str,
    inputs: &[String],
    script_args: &[String],
) -> io::Result<()> {
    let mut text = String::new();
    text.push_str("#!/usr/bin/env bash\n");
    text.push_str("set -euo pipefail\n\n");

    let script_q = shell_quote_os(script.as_os_str());
    let input_flag_q = shell_quote(input_flag);
    let script_args_q = script_args
        .iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>();
    let template_tokens = parse_template_tokens(input_flag);
    let has_template = template_tokens.iter().any(|t| t.contains("$1"));
    let positional_slot = parse_positional_slot(input_flag);

    for input in inputs {
        let input_q = shell_quote(input);
        if let Some(slot) = positional_slot {
            let mut args = script_args_q.clone();
            let idx = slot.saturating_sub(1).min(args.len());
            args.insert(idx, input_q);

            if args.is_empty() {
                text.push_str(&format!("bash {}\n", script_q));
            } else {
                text.push_str(&format!("bash {} {}\n", script_q, args.join(" ")));
            }
        } else if has_template {
            let mut args = template_tokens
                .iter()
                .map(|t| shell_quote(&t.replace("$1", input)))
                .collect::<Vec<_>>();
            args.extend(script_args_q.iter().cloned());
            text.push_str(&format!("bash {} {}\n", script_q, args.join(" ")));
        } else if script_args_q.is_empty() {
            text.push_str(&format!("bash {} {} {}\n", script_q, input_flag_q, input_q));
        } else {
            text.push_str(&format!(
                "bash {} {} {} {}\n",
                script_q,
                input_flag_q,
                input_q,
                script_args_q.join(" ")
            ));
        }
    }

    fs::write(output_path, text)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(output_path, perms)?;
    }

    Ok(())
}

fn submit_job(submit: &str, job_script: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let parts = shlex::split(submit).ok_or_else(|| {
        format!(
            "could not parse --submit command string (check shell quoting): {}",
            submit
        )
    })?;

    let (program, args) = parts
        .split_first()
        .ok_or_else(|| "--submit cannot be empty".to_string())?;

    let output = Command::new(program).args(args).arg(job_script).output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        print!("{}", stdout);
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!(
            "{} failed for {}: {}",
            program,
            job_script.display(),
            stderr.trim()
        )
        .into())
    }
}

fn shell_quote_path(path: &Path) -> String {
    shell_quote_os(path.as_os_str())
}

fn parse_positional_slot(input_flag: &str) -> Option<usize> {
    let idx = input_flag.strip_prefix('$')?.parse::<usize>().ok()?;
    if idx == 0 {
        None
    } else {
        Some(idx)
    }
}

fn parse_template_tokens(input_flag: &str) -> Vec<String> {
    shlex::split(input_flag).unwrap_or_else(|| vec![input_flag.to_string()])
}

fn has_glob_meta(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('[')
}

fn shell_quote_os(s: &OsStr) -> String {
    shell_quote(&s.to_string_lossy())
}

fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    if s.bytes().all(|b| b.is_ascii_alphanumeric() || b"@%_+=:,./-".contains(&b)) {
        return s.to_string();
    }
    let escaped = s.replace('\'', "'\\''");
    format!("'{}'", escaped)
}
