import os
import sys
import time
import json
from datetime import datetime

# Color definitions for terminal output
COLOR_RESET = "\033[0m"
COLOR_CLI = "\033[36m"     # Cyan
COLOR_LLM_REQ = "\033[33m" # Yellow
COLOR_LLM_RES = "\033[32m" # Green
COLOR_WARN = "\033[31m"    # Red
COLOR_INFO = "\033[90m"    # Gray

def get_log_dir():
    appdata = os.environ.get("APPDATA")
    if not appdata:
        print("Error: APPDATA environment variable not found.")
        sys.exit(1)
    return os.path.join(appdata, "Block", "goose", "data", "logs")

def get_latest_cli_log(log_dir):
    cli_dir = os.path.join(log_dir, "cli")
    if not os.path.exists(cli_dir):
        return None
    
    log_files = []
    for root, _, files in os.walk(cli_dir):
        for f in files:
            if f.endswith(".log"):
                path = os.path.join(root, f)
                log_files.append((os.path.getmtime(path), path))
    
    if not log_files:
        return None
    
    # Return path of newest log file
    return sorted(log_files, key=lambda x: x[0])[-1][1]

class LogTailer:
    def __init__(self, filepath):
        self.filepath = filepath
        self.file = None
        self.last_inode = None
        self.open_file()

    def open_file(self):
        if self.file:
            self.file.close()
        
        if not os.path.exists(self.filepath):
            self.file = None
            return False

        self.file = open(self.filepath, "r", encoding="utf-8", errors="ignore")
        # Seek to the end by default so we only stream new logs
        self.file.seek(0, os.SEEK_END)
        
        try:
            stat = os.stat(self.filepath)
            self.last_inode = stat.st_ino
        except Exception:
            self.last_inode = None
        return True

    def read_lines(self):
        if not self.file or not os.path.exists(self.filepath):
            self.open_file()
            if not self.file:
                return []

        # Check if the file has been recreated/rotated (e.g. inode changes or size shrinks)
        try:
            stat = os.stat(self.filepath)
            if self.last_inode and stat.st_ino != self.last_inode:
                self.open_file()
            elif stat.st_size < self.file.tell():
                # File was truncated
                self.file.seek(0)
        except Exception:
            pass

        lines = self.file.readlines()
        return lines

def format_cli_line(line):
    try:
        data = json.loads(line.strip())
        timestamp = data.get("timestamp", datetime.now().isoformat())
        # Parse standard ISO timestamp to shorter human time
        t_parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).strftime("%H:%M:%S")
        
        level = data.get("level", "INFO").upper()
        message = data.get("fields", {}).get("message", "")
        target = data.get("target", "")
        
        # Color coding by level
        level_color = COLOR_CLI
        if level in ["WARN", "WARNING"]:
            level_color = COLOR_WARN
        
        return f"{COLOR_INFO}[{t_parsed}]{COLOR_RESET} {level_color}[{level}]{COLOR_RESET} {COLOR_INFO}({target}){COLOR_RESET} {message}"
    except Exception:
        return f"{COLOR_CLI}[CLI LOG]{COLOR_RESET} {line.strip()}"

def format_llm_line(line):
    try:
        data = json.loads(line.strip())
        
        # Determine if it's request or response
        # llm_request.0.jsonl rows typically record completed calls or structured pairs
        timestamp = data.get("timestamp", datetime.now().isoformat())
        t_parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).strftime("%H:%M:%S")
        
        model = data.get("model", "unknown-model")
        duration = data.get("duration_ms", 0) / 1000.0
        
        request = data.get("request", {})
        response = data.get("response", {})
        
        # Extract prompt overview
        messages = request.get("messages", [])
        last_user = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                last_user = msg.get("content", "")
                if isinstance(last_user, list):
                    last_user = " ".join([c.get("text", "") for c in last_user if c.get("type") == "text"])
                break
        
        # Extract response content
        choices = response.get("choices", [])
        assistant_text = ""
        tool_calls = []
        
        if choices:
            msg = choices[0].get("message", {})
            assistant_text = msg.get("content", "") or ""
            tool_calls = msg.get("tool_calls", []) or []
            
        usage = response.get("usage", {})
        tokens_in = usage.get("prompt_tokens", 0)
        tokens_out = usage.get("completion_tokens", 0)
        
        out = []
        out.append(f"{COLOR_INFO}[{t_parsed}]{COLOR_RESET} {COLOR_LLM_REQ}[LLM REQ]{COLOR_RESET} model: {model} | latency: {duration:.2f}s | tokens: {tokens_in}in/{tokens_out}out")
        if last_user:
            clean_user = last_user.strip().replace("\n", " ")
            if len(clean_user) > 120:
                clean_user = clean_user[:117] + "..."
            out.append(f"   {COLOR_WARN}User Prompt:{COLOR_RESET} {clean_user}")
            
        if assistant_text:
            clean_text = assistant_text.strip().replace("\n", " ")
            if len(clean_text) > 120:
                clean_text = clean_text[:117] + "..."
            out.append(f"   {COLOR_LLM_RES}Response:{COLOR_RESET} {clean_text}")
            
        for tool in tool_calls:
            t_func = tool.get("function", {})
            t_name = t_func.get("name", "unknown")
            t_args = t_func.get("arguments", "")
            out.append(f"   {COLOR_LLM_RES}Calling Tool:{COLOR_RESET} {t_name}({t_args.strip()})")
            
        return "\n".join(out)
    except Exception as e:
        return f"{COLOR_LLM_REQ}[LLM LOG]{COLOR_RESET} Raw Payload: {line.strip()[:150]}... (parse err: {e})"

def main():
    # Enable ANSI escape sequences on Windows Command Prompt / PowerShell
    os.system("")
    
    log_dir = get_log_dir()
    print(f"{COLOR_INFO}Watching log directory: {log_dir}{COLOR_RESET}")
    
    current_cli_log = get_latest_cli_log(log_dir)
    cli_tailer = LogTailer(current_cli_log) if current_cli_log else None
    
    if current_cli_log:
        print(f"{COLOR_INFO}Tailing CLI logs from: {os.path.basename(current_cli_log)}{COLOR_RESET}")
    else:
        print(f"{COLOR_WARN}No CLI log file found yet. Waiting for CLI activity...{COLOR_RESET}")
        
    llm_log_path = os.path.join(log_dir, "llm_request.0.jsonl")
    llm_tailer = LogTailer(llm_log_path)
    print(f"{COLOR_INFO}Tailing LLM requests from: {os.path.basename(llm_log_path)}{COLOR_RESET}\n")
    
    last_check_time = time.time()
    
    try:
        while True:
            # 1. Periodically check if a newer CLI log file was created (indicates a new run)
            if time.time() - last_check_time > 2.0:
                new_cli_log = get_latest_cli_log(log_dir)
                if new_cli_log and new_cli_log != current_cli_log:
                    current_cli_log = new_cli_log
                    print(f"\n{COLOR_INFO}--- Switched to new CLI session: {os.path.basename(current_cli_log)} ---{COLOR_RESET}\n")
                    cli_tailer = LogTailer(current_cli_log)
                last_check_time = time.time()
            
            # 2. Tail CLI logs
            if cli_tailer:
                for line in cli_tailer.read_lines():
                    if line.strip():
                        print(format_cli_line(line))
            
            # 3. Tail LLM API requests
            for line in llm_tailer.read_lines():
                if line.strip():
                    print(format_llm_line(line))
                    
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print(f"\n{COLOR_INFO}Stopped watching logs.{COLOR_RESET}")
        sys.exit(0)

if __name__ == "__main__":
    main()
