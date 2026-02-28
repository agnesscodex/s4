use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone)]
struct AliasConfig {
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
    path_style: bool,
}

#[derive(Debug, Default)]
struct AppConfig {
    aliases: BTreeMap<String, AliasConfig>,
}

#[derive(Debug, Default)]
struct GlobalOpts {
    config_dir: Option<PathBuf>,
    json: bool,
    debug: bool,
    insecure: bool,
}

#[derive(Debug)]
struct S3Target {
    alias: String,
    bucket: Option<String>,
    key: Option<String>,
}

#[derive(Debug)]
struct Endpoint {
    scheme: String,
    host: String,
    base_path: String,
}

#[derive(Debug)]
struct SignatureParts {
    amz_date: String,
    authorization: String,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args: Vec<String> = env::args().collect();
    if args.len() == 1 {
        print_help();
        return Ok(());
    }
    args.remove(0);

    let (opts, rest) = parse_globals(args)?;
    if rest.is_empty() {
        print_help();
        return Ok(());
    }

    if rest[0] == "--help" || rest[0] == "-h" {
        print_help();
        return Ok(());
    }
    if rest[0] == "--version" || rest[0] == "-v" || rest[0] == "version" {
        println!("s4 {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let config_path = resolve_config_path(opts.config_dir.as_deref())?;
    let mut config = load_config(&config_path)?;

    if opts.debug {
        eprintln!("[debug] config: {}", config_path.display());
    }
    if opts.insecure {
        eprintln!("[warn] --insecure is not implemented yet");
    }

    match rest[0].as_str() {
        "alias" => handle_alias(&rest[1..], &mut config, &config_path, opts.json),
        "ls" | "mb" | "rb" | "put" | "get" | "rm" | "stat" | "cat" | "sync" | "mirror" | "cp"
        | "mv" => handle_s3_command(&rest, &config, opts.json, opts.debug),
        _ => Err(format!("unknown command: {}", rest[0])),
    }
}

fn parse_globals(args: Vec<String>) -> Result<(GlobalOpts, Vec<String>), String> {
    let mut opts = GlobalOpts::default();
    let mut rest = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-C" | "--config-dir" => {
                let next = args.get(i + 1).ok_or("--config-dir expects a value")?;
                opts.config_dir = Some(PathBuf::from(next));
                i += 2;
            }
            "--json" => {
                opts.json = true;
                i += 1;
            }
            "--debug" => {
                opts.debug = true;
                i += 1;
            }
            "--insecure" => {
                opts.insecure = true;
                i += 1;
            }
            "--help" | "-h" | "--version" | "-v" => {
                rest.extend_from_slice(&args[i..]);
                break;
            }
            x if x.starts_with('-') => return Err(format!("unknown global flag: {x}")),
            _ => {
                rest.extend_from_slice(&args[i..]);
                break;
            }
        }
    }

    Ok((opts, rest))
}

fn handle_alias(
    args: &[String],
    config: &mut AppConfig,
    config_path: &Path,
    json: bool,
) -> Result<(), String> {
    if args.is_empty() {
        return Err("usage: s4 alias <set|ls|rm> ...".to_string());
    }

    match args[0].as_str() {
        "set" => {
            if args.len() < 5 {
                return Err("usage: s4 alias set <name> <endpoint> <access> <secret> [--region r] [--path-style]".to_string());
            }
            let mut region = "us-east-1".to_string();
            let mut path_style = false;
            let mut i = 5;
            while i < args.len() {
                match args[i].as_str() {
                    "--region" => {
                        region = args
                            .get(i + 1)
                            .ok_or("--region expects a value")?
                            .to_string();
                        i += 2;
                    }
                    "--path-style" => {
                        path_style = true;
                        i += 1;
                    }
                    other => return Err(format!("unknown alias set flag: {other}")),
                }
            }

            config.aliases.insert(
                args[1].clone(),
                AliasConfig {
                    endpoint: args[2].clone(),
                    access_key: args[3].clone(),
                    secret_key: args[4].clone(),
                    region,
                    path_style,
                },
            );
            save_config(config_path, config)?;
            if json {
                println!("{{\"status\":\"ok\",\"alias\":\"{}\"}}", args[1]);
            } else {
                println!("Alias '{}' saved", args[1]);
            }
            Ok(())
        }
        "ls" => {
            if json {
                print!("[");
                for (idx, (name, alias)) in config.aliases.iter().enumerate() {
                    if idx > 0 {
                        print!(",");
                    }
                    print!(
                        "{{\"name\":\"{}\",\"endpoint\":\"{}\",\"region\":\"{}\",\"path_style\":{}}}",
                        escape_json(name),
                        escape_json(&alias.endpoint),
                        escape_json(&alias.region),
                        alias.path_style
                    );
                }
                println!("]");
            } else {
                for (name, alias) in &config.aliases {
                    println!(
                        "{name}\t{}\t{}\tpath_style={}",
                        alias.endpoint, alias.region, alias.path_style
                    );
                }
            }
            Ok(())
        }
        "rm" => {
            let name = args.get(1).ok_or("usage: s4 alias rm <name>")?;
            let existed = config.aliases.remove(name).is_some();
            save_config(config_path, config)?;
            if json {
                println!(
                    "{{\"status\":\"ok\",\"alias\":\"{}\",\"removed\":{}}}",
                    escape_json(name),
                    existed
                );
            } else if existed {
                println!("Alias '{name}' removed");
            } else {
                println!("Alias '{name}' not found");
            }
            Ok(())
        }
        _ => Err("usage: s4 alias <set|ls|rm> ...".to_string()),
    }
}

fn handle_s3_command(
    args: &[String],
    config: &AppConfig,
    json: bool,
    debug: bool,
) -> Result<(), String> {
    let command = &args[0];
    let target_idx = if command == "put" { 2 } else { 1 };
    if command != "sync"
        && command != "mirror"
        && command != "cp"
        && command != "mv"
        && args.len() <= target_idx
    {
        return Err(format!("usage: s4 {command} ..."));
    }

    if command == "cp" || command == "mv" {
        if args.len() < 3 {
            return Err(format!("usage: s4 {command} <source> <target>"));
        }
        return cmd_cp_mv(command, config, &args[1], &args[2], json, debug);
    }

    if command == "sync" || command == "mirror" {
        if args.len() < 3 {
            return Err(
                "usage: s4 sync|mirror <src_alias/bucket[/prefix]> <dst_alias/bucket[/prefix]>"
                    .to_string(),
            );
        }
        let src = parse_target(&args[1])?;
        let dst = parse_target(&args[2])?;
        return cmd_sync(config, &src, &dst, json, debug);
    }

    let target = parse_target(&args[target_idx])?;
    let alias = config
        .aliases
        .get(&target.alias)
        .ok_or_else(|| format!("unknown alias: {}", target.alias))?;

    match command.as_str() {
        "ls" => cmd_ls(alias, &target, json, debug),
        "mb" => {
            let bucket = req_bucket(&target, "mb")?;
            s3_request(alias, "PUT", &bucket, None, "", None, None, debug)?;
            print_status(json, "created", &bucket);
            Ok(())
        }
        "rb" => {
            let bucket = req_bucket(&target, "rb")?;
            s3_request(alias, "DELETE", &bucket, None, "", None, None, debug)?;
            print_status(json, "deleted", &bucket);
            Ok(())
        }
        "put" => {
            if args.len() < 3 {
                return Err("usage: s4 put <source_file> <alias/bucket/key>".to_string());
            }
            let source = PathBuf::from(&args[1]);
            if !source.exists() {
                return Err(format!("source file not found: {}", source.display()));
            }
            let bucket = req_bucket(&target, "put")?;
            let key = req_key(&target, "put")?;
            s3_request(
                alias,
                "PUT",
                &bucket,
                Some(&key),
                "",
                Some(&source),
                None,
                debug,
            )?;
            if json {
                println!(
                    "{{\"uploaded\":{{\"bucket\":\"{}\",\"key\":\"{}\"}}}}",
                    escape_json(&bucket),
                    escape_json(&key)
                );
            } else {
                println!("Uploaded '{}' to '{}/{}'", source.display(), bucket, key);
            }
            Ok(())
        }
        "get" => {
            if args.len() < 3 {
                return Err("usage: s4 get <alias/bucket/key> <destination_file>".to_string());
            }
            let bucket = req_bucket(&target, "get")?;
            let key = req_key(&target, "get")?;
            let destination = PathBuf::from(&args[2]);
            if let Some(parent) = destination.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
            }
            s3_request(
                alias,
                "GET",
                &bucket,
                Some(&key),
                "",
                None,
                Some(&destination),
                debug,
            )?;
            if json {
                println!(
                    "{{\"downloaded\":{{\"bucket\":\"{}\",\"key\":\"{}\",\"to\":\"{}\"}}}}",
                    escape_json(&bucket),
                    escape_json(&key),
                    escape_json(&destination.display().to_string())
                );
            } else {
                println!(
                    "Downloaded '{}/{}' to '{}'",
                    bucket,
                    key,
                    destination.display()
                );
            }
            Ok(())
        }
        "rm" => {
            let bucket = req_bucket(&target, "rm")?;
            let key = req_key(&target, "rm")?;
            s3_request(alias, "DELETE", &bucket, Some(&key), "", None, None, debug)?;
            if json {
                println!(
                    "{{\"deleted\":{{\"bucket\":\"{}\",\"key\":\"{}\"}}}}",
                    escape_json(&bucket),
                    escape_json(&key)
                );
            } else {
                println!("Deleted '{}/{}'", bucket, key);
            }
            Ok(())
        }
        "stat" => {
            let bucket = req_bucket(&target, "stat")?;
            let key = req_key(&target, "stat")?;
            let headers = s3_request(alias, "HEAD", &bucket, Some(&key), "", None, None, debug)?;
            if json {
                println!(
                    "{{\"bucket\":\"{}\",\"key\":\"{}\",\"headers\":\"{}\"}}",
                    escape_json(&bucket),
                    escape_json(&key),
                    escape_json(&headers)
                );
            } else {
                println!("{}", headers);
            }
            Ok(())
        }
        "cat" => {
            let bucket = req_bucket(&target, "cat")?;
            let key = req_key(&target, "cat")?;
            let body = s3_request(alias, "GET", &bucket, Some(&key), "", None, None, debug)?;
            print!("{}", body);
            Ok(())
        }
        "sync" | "mirror" => unreachable!(),
        "cp" | "mv" => unreachable!(),
        _ => Err(format!("unsupported command: {command}")),
    }
}

fn cmd_sync(
    config: &AppConfig,
    source: &S3Target,
    destination: &S3Target,
    json: bool,
    debug: bool,
) -> Result<(), String> {
    let src_alias = config
        .aliases
        .get(&source.alias)
        .ok_or_else(|| format!("unknown alias: {}", source.alias))?;
    let dst_alias = config
        .aliases
        .get(&destination.alias)
        .ok_or_else(|| format!("unknown alias: {}", destination.alias))?;

    let src_bucket = req_bucket(source, "sync")?;
    let dst_bucket = req_bucket(destination, "sync")?;
    let src_prefix = source.key.clone().unwrap_or_default();
    let dst_prefix = destination.key.clone().unwrap_or_default();

    let keys = list_object_keys(src_alias, &src_bucket, &src_prefix, debug)?;
    let temp_root = env::temp_dir().join(format!("s4-sync-{}", std::process::id()));
    fs::create_dir_all(&temp_root).map_err(|e| e.to_string())?;

    let mut copied = 0usize;
    for (idx, key) in keys.iter().enumerate() {
        let dest_key = sync_destination_key(key, &src_prefix, &dst_prefix);
        let temp_file = temp_root.join(format!("obj-{idx}"));
        s3_request(
            src_alias,
            "GET",
            &src_bucket,
            Some(key),
            "",
            None,
            Some(&temp_file),
            debug,
        )?;
        s3_request(
            dst_alias,
            "PUT",
            &dst_bucket,
            Some(&dest_key),
            "",
            Some(&temp_file),
            None,
            debug,
        )?;
        copied += 1;
    }

    fs::remove_dir_all(&temp_root).ok();

    if json {
        println!(
            "{{\"status\":\"ok\",\"copied\":{},\"src\":\"{}\",\"dst\":\"{}\"}}",
            copied,
            escape_json(&format!("{}/{}", source.alias, src_bucket)),
            escape_json(&format!("{}/{}", destination.alias, dst_bucket))
        );
    } else {
        println!(
            "Synced {} object(s) from {}/{} to {}/{}",
            copied, source.alias, src_bucket, destination.alias, dst_bucket
        );
    }

    Ok(())
}

fn cmd_cp_mv(
    command: &str,
    config: &AppConfig,
    source: &str,
    target: &str,
    json: bool,
    debug: bool,
) -> Result<(), String> {
    let src = classify_ref(config, source);
    let dst = classify_ref(config, target);

    match (&src, &dst) {
        (ObjectRef::Local(src_path), ObjectRef::S3(dst_s3)) => {
            let body_path = PathBuf::from(src_path);
            if !body_path.exists() {
                return Err(format!("source file not found: {}", body_path.display()));
            }
            s3_request(
                &dst_s3.alias,
                "PUT",
                &dst_s3.bucket,
                Some(&dst_s3.key),
                "",
                Some(&body_path),
                None,
                debug,
            )?;
            if command == "mv" {
                fs::remove_file(&body_path).map_err(|e| e.to_string())?;
            }
        }
        (ObjectRef::S3(src_s3), ObjectRef::Local(dst_path)) => {
            let out = PathBuf::from(dst_path);
            if let Some(parent) = out.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
            }
            s3_request(
                &src_s3.alias,
                "GET",
                &src_s3.bucket,
                Some(&src_s3.key),
                "",
                None,
                Some(&out),
                debug,
            )?;
            if command == "mv" {
                s3_request(
                    &src_s3.alias,
                    "DELETE",
                    &src_s3.bucket,
                    Some(&src_s3.key),
                    "",
                    None,
                    None,
                    debug,
                )?;
            }
        }
        (ObjectRef::S3(src_s3), ObjectRef::S3(dst_s3)) => {
            copy_object_s3_to_s3(src_s3, dst_s3, debug)?;
            if command == "mv" {
                s3_request(
                    &src_s3.alias,
                    "DELETE",
                    &src_s3.bucket,
                    Some(&src_s3.key),
                    "",
                    None,
                    None,
                    debug,
                )?;
            }
        }
        (ObjectRef::Local(src_path), ObjectRef::Local(dst_path)) => {
            fs::copy(src_path, dst_path).map_err(|e| e.to_string())?;
            if command == "mv" {
                fs::remove_file(src_path).map_err(|e| e.to_string())?;
            }
        }
    }

    if json {
        println!(
            "{{\"status\":\"ok\",\"command\":\"{}\",\"source\":\"{}\",\"target\":\"{}\"}}",
            escape_json(command),
            escape_json(source),
            escape_json(target)
        );
    } else {
        println!("{}: {} -> {}", command, source, target);
    }
    Ok(())
}

#[derive(Clone)]
struct S3ObjectRef {
    alias: AliasConfig,
    bucket: String,
    key: String,
}

enum ObjectRef {
    S3(S3ObjectRef),
    Local(String),
}

fn classify_ref(config: &AppConfig, value: &str) -> ObjectRef {
    if let Ok(t) = parse_target(value) {
        if let Some(alias) = config.aliases.get(&t.alias) {
            if let (Some(bucket), Some(key)) = (t.bucket, t.key) {
                return ObjectRef::S3(S3ObjectRef {
                    alias: alias.clone(),
                    bucket,
                    key,
                });
            }
        }
    }
    ObjectRef::Local(value.to_string())
}

fn copy_object_s3_to_s3(src: &S3ObjectRef, dst: &S3ObjectRef, debug: bool) -> Result<(), String> {
    let copy_source = format!(
        "/{}/{}",
        uri_encode_segment(&src.bucket),
        uri_encode_path(&src.key)
    );
    let headers = vec![format!("x-amz-copy-source: {}", copy_source)];
    s3_request_with_headers(
        &dst.alias,
        "PUT",
        &dst.bucket,
        Some(&dst.key),
        "",
        None,
        None,
        &headers,
        debug,
    )?;
    Ok(())
}

fn cmd_ls(alias: &AliasConfig, target: &S3Target, json: bool, debug: bool) -> Result<(), String> {
    match &target.bucket {
        None => {
            let body = s3_request(alias, "GET", "", None, "", None, None, debug)?;
            if json {
                println!("{{\"xml\":\"{}\"}}", escape_json(&body));
            } else {
                println!("{body}");
            }
        }
        Some(bucket) => {
            let body = s3_request(alias, "GET", bucket, None, "list-type=2", None, None, debug)?;
            if json {
                println!("{{\"xml\":\"{}\"}}", escape_json(&body));
            } else {
                println!("{body}");
            }
        }
    }
    Ok(())
}

fn list_object_keys(
    alias: &AliasConfig,
    bucket: &str,
    prefix: &str,
    debug: bool,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let mut query = String::from("list-type=2");
        if !prefix.is_empty() {
            query.push_str("&prefix=");
            query.push_str(&uri_encode_path(prefix));
        }
        if let Some(token) = continuation.as_ref() {
            query.push_str("&continuation-token=");
            query.push_str(&uri_encode_path(token));
        }

        let body = s3_request(alias, "GET", bucket, None, &query, None, None, debug)?;
        keys.extend(
            extract_tag_values(&body, "Key")
                .into_iter()
                .map(|k| xml_unescape(&k)),
        );

        let is_truncated = extract_tag_values(&body, "IsTruncated")
            .into_iter()
            .next()
            .unwrap_or_else(|| "false".to_string())
            .trim()
            .eq("true");

        if is_truncated {
            continuation = extract_tag_values(&body, "NextContinuationToken")
                .into_iter()
                .next()
                .map(|v| xml_unescape(&v));
            if continuation.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    Ok(keys)
}

fn sync_destination_key(source_key: &str, src_prefix: &str, dst_prefix: &str) -> String {
    let normalized_src = src_prefix.trim_matches('/');
    let mut relative = source_key.to_string();

    if !normalized_src.is_empty() {
        if source_key == normalized_src {
            relative.clear();
        } else if let Some(rest) = source_key.strip_prefix(&(normalized_src.to_string() + "/")) {
            relative = rest.to_string();
        }
    }

    let normalized_dst = dst_prefix.trim_matches('/');
    if normalized_dst.is_empty() {
        return relative;
    }
    if relative.is_empty() {
        return normalized_dst.to_string();
    }

    format!("{normalized_dst}/{relative}")
}

fn extract_tag_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");

    let mut out = Vec::new();
    let mut remaining = xml;

    while let Some(start) = remaining.find(&open) {
        let after_open = &remaining[start + open.len()..];
        let Some(end) = after_open.find(&close) else {
            break;
        };
        out.push(after_open[..end].to_string());
        remaining = &after_open[end + close.len()..];
    }

    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn req_bucket(target: &S3Target, cmd: &str) -> Result<String, String> {
    target
        .bucket
        .clone()
        .ok_or_else(|| format!("{cmd} requires alias/bucket"))
}

fn req_key(target: &S3Target, cmd: &str) -> Result<String, String> {
    target
        .key
        .clone()
        .ok_or_else(|| format!("{cmd} requires alias/bucket/key"))
}

fn s3_request(
    alias: &AliasConfig,
    method: &str,
    bucket: &str,
    key: Option<&str>,
    query: &str,
    upload_file: Option<&Path>,
    output_file: Option<&Path>,
    debug: bool,
) -> Result<String, String> {
    s3_request_with_headers(
        alias,
        method,
        bucket,
        key,
        query,
        upload_file,
        output_file,
        &[],
        debug,
    )
}

fn s3_request_with_headers(
    alias: &AliasConfig,
    method: &str,
    bucket: &str,
    key: Option<&str>,
    query: &str,
    upload_file: Option<&Path>,
    output_file: Option<&Path>,
    extra_headers: &[String],
    debug: bool,
) -> Result<String, String> {
    let endpoint = parse_endpoint(&alias.endpoint)?;
    let mut uri_path = endpoint.base_path.clone();

    if alias.path_style {
        if !bucket.is_empty() {
            uri_path.push('/');
            uri_path.push_str(&uri_encode_segment(bucket));
        }
        if let Some(k) = key {
            uri_path.push('/');
            uri_path.push_str(&uri_encode_path(k));
        }
    } else {
        return Err("only --path-style aliases are supported in this build".to_string());
    }

    if uri_path.is_empty() {
        uri_path = "/".to_string();
    }

    let payload_hash = payload_hash(upload_file)?;
    let sign = sign_v4(
        method,
        &uri_path,
        query,
        &endpoint.host,
        &alias.region,
        &alias.access_key,
        &alias.secret_key,
        &payload_hash,
    )?;

    let mut url = format!("{}://{}{}", endpoint.scheme, endpoint.host, uri_path);
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }

    let mut cmd = Command::new("curl");
    cmd.arg("-sS").arg(&url);
    if method != "HEAD" {
        cmd.arg("-X").arg(method);
    }
    cmd.arg("-H")
        .arg(format!("Host: {}", endpoint.host))
        .arg("-H")
        .arg(format!("x-amz-date: {}", sign.amz_date))
        .arg("-H")
        .arg(format!("x-amz-content-sha256: {}", payload_hash))
        .arg("-H")
        .arg(format!("Authorization: {}", sign.authorization));

    for header in extra_headers {
        cmd.arg("-H").arg(header);
    }

    if let Some(file) = upload_file {
        cmd.arg("--data-binary").arg(format!("@{}", file.display()));
    }

    if method == "HEAD" {
        // Use curl native HEAD mode instead of `-X HEAD` + body suppression.
        // This avoids curl(18) "transfer closed with bytes remaining" on servers
        // that return Content-Length for HEAD responses.
        cmd.arg("-I");
    } else if let Some(out) = output_file {
        cmd.arg("-o").arg(out);
    }

    if debug {
        eprintln!("[debug] request: {} {}", method, url);
    }

    cmd.arg("-w").arg("\nHTTPSTATUS:%{http_code}");

    let output = cmd.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(format!("request execution failed: {}", stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let (body, status_part) = stdout
        .rsplit_once("\nHTTPSTATUS:")
        .ok_or_else(|| "unable to parse HTTP status".to_string())?;
    let status = status_part.trim();
    if !status.starts_with('2') {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(format!(
            "request failed with status {status}: body='{}' stderr='{}'",
            body.trim(),
            stderr.trim()
        ));
    }

    Ok(body.to_string())
}

fn sign_v4(
    method: &str,
    uri_path: &str,
    query: &str,
    host: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    payload_hash: &str,
) -> Result<SignatureParts, String> {
    let py = r#"
import sys, hmac, hashlib, datetime
method, path, query, host, region, access, secret, payload_hash = sys.argv[1:]
service = 's3'
amz_date = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
date_stamp = amz_date[:8]
canonical_headers = f'host:{host}\n' + f'x-amz-content-sha256:{payload_hash}\n' + f'x-amz-date:{amz_date}\n'
signed_headers = 'host;x-amz-content-sha256;x-amz-date'
canonical_request = '\n'.join([method, path, query, canonical_headers, signed_headers, payload_hash])
algorithm = 'AWS4-HMAC-SHA256'
credential_scope = f'{date_stamp}/{region}/{service}/aws4_request'
string_to_sign = '\n'.join([algorithm, amz_date, credential_scope, hashlib.sha256(canonical_request.encode()).hexdigest()])
def sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()
k_date = sign(('AWS4' + secret).encode(), date_stamp)
k_region = sign(k_date, region)
k_service = sign(k_region, service)
k_signing = sign(k_service, 'aws4_request')
signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
auth = f'{algorithm} Credential={access}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}'
print(amz_date)
print(auth)
"#;

    let out = Command::new("python3")
        .arg("-c")
        .arg(py)
        .arg(method)
        .arg(uri_path)
        .arg(query)
        .arg(host)
        .arg(region)
        .arg(access_key)
        .arg(secret_key)
        .arg(payload_hash)
        .output()
        .map_err(|e| e.to_string())?;

    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).to_string());
    }

    let lines: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(ToString::to_string)
        .collect();
    if lines.len() < 2 {
        return Err("signature helper returned unexpected output".to_string());
    }

    Ok(SignatureParts {
        amz_date: lines[0].clone(),
        authorization: lines[1].clone(),
    })
}

fn payload_hash(upload_file: Option<&Path>) -> Result<String, String> {
    if let Some(path) = upload_file {
        let out = Command::new("python3")
            .arg("-c")
            .arg("import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())")
            .arg(path)
            .output()
            .map_err(|e| e.to_string())?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).to_string());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Ok("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string())
    }
}

fn parse_endpoint(raw: &str) -> Result<Endpoint, String> {
    let (scheme, rest) = if let Some(v) = raw.strip_prefix("http://") {
        ("http", v)
    } else if let Some(v) = raw.strip_prefix("https://") {
        ("https", v)
    } else {
        return Err("endpoint must start with http:// or https://".to_string());
    };

    let mut parts = rest.splitn(2, '/');
    let host = parts.next().unwrap_or("").to_string();
    if host.is_empty() {
        return Err("endpoint host is empty".to_string());
    }
    let base_path = match parts.next() {
        Some(v) if !v.is_empty() => format!("/{}", v.trim_end_matches('/')),
        _ => "".to_string(),
    };

    Ok(Endpoint {
        scheme: scheme.to_string(),
        host,
        base_path,
    })
}

fn resolve_config_path(custom_dir: Option<&Path>) -> Result<PathBuf, String> {
    match custom_dir {
        Some(p) => Ok(p.join("config.toml")),
        None => {
            let home = env::var("HOME").map_err(|_| "HOME is not set".to_string())?;
            Ok(PathBuf::from(home).join(".s4").join("config.toml"))
        }
    }
}

fn load_config(path: &Path) -> Result<AppConfig, String> {
    if !path.exists() {
        return Ok(AppConfig::default());
    }

    let mut file = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut s = String::new();
    file.read_to_string(&mut s).map_err(|e| e.to_string())?;
    parse_config(&s)
}

fn save_config(path: &Path, cfg: &AppConfig) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }

    let text = serialize_config(cfg);
    fs::write(path, text).map_err(|e| e.to_string())
}

fn parse_config(text: &str) -> Result<AppConfig, String> {
    let mut cfg = AppConfig::default();
    for (ln, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 6 {
            return Err(format!("invalid config at line {}", ln + 1));
        }
        cfg.aliases.insert(
            parts[0].to_string(),
            AliasConfig {
                endpoint: parts[1].to_string(),
                access_key: parts[2].to_string(),
                secret_key: parts[3].to_string(),
                region: parts[4].to_string(),
                path_style: parts[5] == "1",
            },
        );
    }
    Ok(cfg)
}

fn serialize_config(cfg: &AppConfig) -> String {
    let mut out = String::new();
    for (name, a) in &cfg.aliases {
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            name,
            a.endpoint,
            a.access_key,
            a.secret_key,
            a.region,
            if a.path_style { "1" } else { "0" }
        ));
    }
    out
}

fn parse_target(input: &str) -> Result<S3Target, String> {
    let mut parts = input.splitn(3, '/');
    let alias = parts
        .next()
        .ok_or_else(|| "target must start with alias".to_string())?
        .to_string();
    if alias.is_empty() {
        return Err("target alias is empty".to_string());
    }
    let bucket = parts.next().map(ToString::to_string);
    let key = parts.next().map(ToString::to_string);
    Ok(S3Target { alias, bucket, key })
}

fn uri_encode_segment(s: &str) -> String {
    uri_encode_path(s)
}

fn uri_encode_path(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '~' || c == '/' {
            out.push(c);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn print_status(json: bool, field: &str, value: &str) {
    if json {
        println!("{{\"{}\":\"{}\"}}", escape_json(field), escape_json(value));
    } else {
        println!("{field}: {value}");
    }
}

fn print_help() {
    println!(
        "s4 - S3 client utility in Rust\n\nUSAGE:\n  s4 [FLAGS] COMMAND [ARGS]\n\nCOMMANDS:\n  alias      manage aliases in local config\n  ls         list buckets/objects\n  mb         make bucket\n  rb         remove bucket\n  put        upload object\n  get        download object\n  rm         remove object\n  stat       object metadata (raw headers)\n  cat        print object content\n  sync       sync objects from source bucket/prefix to destination\n  mirror     alias for sync (mc-compatible naming)\n  cp         copy object(s) between local and S3\n  mv         move object(s) between local and S3\n  version    print version\n\nFLAGS:\n  -C, --config-dir <DIR>\n  --json\n  --debug\n  --insecure\n  -h, --help\n  -v, --version"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        AliasConfig, AppConfig, extract_tag_values, parse_config, parse_target, serialize_config,
        sync_destination_key, uri_encode_path, xml_unescape,
    };
    use std::collections::BTreeMap;

    #[test]
    fn parse_target_with_key() {
        let t = parse_target("local/bucket/folder/file.txt").expect("target should parse");
        assert_eq!(t.alias, "local");
        assert_eq!(t.bucket.as_deref(), Some("bucket"));
        assert_eq!(t.key.as_deref(), Some("folder/file.txt"));
    }

    #[test]
    fn roundtrip_config() {
        let mut aliases = BTreeMap::new();
        aliases.insert(
            "local".to_string(),
            AliasConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                access_key: "minio".to_string(),
                secret_key: "minio123".to_string(),
                region: "us-east-1".to_string(),
                path_style: true,
            },
        );
        let cfg = AppConfig { aliases };

        let text = serialize_config(&cfg);
        let parsed = parse_config(&text).expect("config should parse");
        assert_eq!(parsed.aliases.len(), 1);
        let alias = parsed.aliases.get("local").expect("alias exists");
        assert!(alias.path_style);
        assert_eq!(alias.region, "us-east-1");
    }

    #[test]
    fn uri_encode_works() {
        assert_eq!(uri_encode_path("a b/c"), "a%20b/c");
    }

    #[test]
    fn extract_xml_keys() {
        let xml = "<ListBucketResult><Contents><Key>a.txt</Key></Contents><Contents><Key>dir/b.txt</Key></Contents></ListBucketResult>";
        let keys = extract_tag_values(xml, "Key");
        assert_eq!(keys, vec!["a.txt".to_string(), "dir/b.txt".to_string()]);
    }

    #[test]
    fn sync_destination_key_respects_prefixes() {
        assert_eq!(
            sync_destination_key("images/cat.jpg", "images", "backup"),
            "backup/cat.jpg"
        );
        assert_eq!(
            sync_destination_key("images/nested/cat.jpg", "", "archive"),
            "archive/images/nested/cat.jpg"
        );
        assert_eq!(sync_destination_key("a.txt", "", ""), "a.txt");
    }

    #[test]
    fn xml_unescape_works() {
        assert_eq!(xml_unescape("a&amp;b&quot;c"), "a&b\"c");
    }
}
