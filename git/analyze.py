#!/usr/bin/env python3

import os
import argparse
from collections import defaultdict, Counter
from datetime import datetime, timezone
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv # For csv.QUOTE_NONNUMERIC etc.

# Third-party libraries
try:
    import git # GitPython
    from git.exc import InvalidGitRepositoryError, NoSuchPathError
except ImportError:
    print("Error: GitPython library not found. Please install it: pip install GitPython")
    exit(1)

try:
    from dateutil import parser as date_parser
except ImportError:
    print("Error: python-dateutil library not found. Please install it: pip install python-dateutil")
    exit(1)

try:
    import yaml # PyYAML for author aliases
except ImportError:
    print("Warning: PyYAML library not found. Author alias file parsing might not work. Install: pip install PyYAML")
    yaml = None

try:
    import pandas as pd
except ImportError:
    print("Warning: pandas library not found. CSV output will be significantly affected or fail. Install: pip install pandas")
    pd = None

try:
    from tabulate import tabulate
except ImportError:
    print("Warning: tabulate library not found. Console reporting will be basic. Install: pip install tabulate")
    tabulate = None

try:
    from tqdm import tqdm
except ImportError:
    print("Warning: tqdm library not found. Progress bars will not be shown. Install: pip install tqdm")
    def tqdm(iterable, *args, **kwargs): # Dummy tqdm
        return iterable

# --- Global Configuration & Constants ---
DEFAULT_MAX_WORKERS = os.cpu_count() or 4

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger("LocalGitAnalyzerScriptFull")

# --- Helper Functions ---
def setup_logging(log_level_str):
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', force=True)
    logger.setLevel(log_level)

def parse_iso_datetime_to_aware_utc(date_str_input, is_end_date=False):
    if not date_str_input: return None
    try:
        dt_naive = datetime.strptime(date_str_input, "%Y-%m-%d")
        if is_end_date:
            dt_aware = datetime(dt_naive.year, dt_naive.month, dt_naive.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
        else:
            dt_aware = datetime(dt_naive.year, dt_naive.month, dt_naive.day, 0, 0, 0, 0, tzinfo=timezone.utc)
        return dt_aware
    except ValueError:
        logger.error(f"Invalid date format: '{date_str_input}'. Please use YYYY-MM-DD.")
        return None

def get_week_year(dt_object):
    if not dt_object or not isinstance(dt_object, datetime):
        if isinstance(dt_object, str):
             try: parsed_dt = date_parser.isoparse(dt_object)
             except ValueError: logger.debug(f"Could not parse date string '{dt_object}' in get_week_year."); return "Unknown Week"
             if parsed_dt: return parsed_dt.strftime("%Y-%U")
        logger.debug(f"Invalid dt_object for get_week_year: {dt_object} (type: {type(dt_object)})")
        return "Unknown Week"
    return dt_object.strftime("%Y-%U")

# --- Author Alias Resolver ---
class AuthorAliasResolver:
    def __init__(self, alias_config_path=None):
        self.aliases = {}
        self.email_to_primary_name = {}
        if alias_config_path and yaml:
            try:
                with open(alias_config_path, 'r', encoding='utf-8') as f:
                    alias_data = yaml.safe_load(f)
                if alias_data and 'author_aliases' in alias_data:
                    for entry in alias_data['author_aliases']:
                        primary_name = entry['name']
                        primary_email = entry.get('primary_email', '')
                        for alias_item in entry['aliases']:
                            self.aliases[alias_item.lower()] = primary_name
                            if '@' in alias_item: self.email_to_primary_name[alias_item.lower()] = primary_name
                        if primary_email: self.email_to_primary_name[primary_email.lower()] = primary_name
                    logger.info(f"Loaded {len(self.aliases) + len(self.email_to_primary_name)} author alias mappings.")
            except FileNotFoundError: logger.error(f"Author alias file not found: '{alias_config_path}'")
            except Exception as e: logger.error(f"Error loading author alias file '{alias_config_path}': {e}")
        elif alias_config_path and not yaml:
            logger.warning("Path to author alias file provided, but PyYAML library not installed. Aliases will not be used.")

    def get_canonical_name(self, name, email):
        name_lower = name.lower(); email_lower = email.lower() if email else ''
        if email_lower in self.email_to_primary_name: return self.email_to_primary_name[email_lower]
        if email_lower in self.aliases: return self.aliases[email_lower]
        if name_lower in self.aliases: return self.aliases[name_lower]
        return name

# --- Local Git Analyzer ---
class LocalGitAnalyzer:
    def __init__(self, repo_path, author_resolver, since_date_obj=None, until_date_obj=None):
        self.repo_path = repo_path
        self.repo_name = os.path.basename(repo_path)
        self.author_resolver = author_resolver
        self.since_date = since_date_obj
        self.until_date = until_date_obj
        try: self.repo = git.Repo(self.repo_path)
        except InvalidGitRepositoryError: logger.error(f"Invalid Git repository: {self.repo_path}"); raise
        except NoSuchPathError: logger.error(f"Path does not exist: {self.repo_path}"); raise

    def get_commit_data(self):
        commits_data = []
        iter_args = ['--all']
        logger.info(f"Fetching ALL commits for {self.repo_name} with GitPython args: {iter_args} (date filtering applied in Python).")
        try:
            all_raw_commits_from_gitpython = list(self.repo.iter_commits(*iter_args))
            logger.info(f"Found {len(all_raw_commits_from_gitpython)} total raw commits for {self.repo_name} from GitPython.")
            filtered_commit_objects = []
            if all_raw_commits_from_gitpython:
                for commit_obj in all_raw_commits_from_gitpython:
                    commit_dt = commit_obj.authored_datetime
                    passes_filter = True
                    if self.since_date and commit_dt < self.since_date: passes_filter = False
                    if self.until_date and commit_dt > self.until_date: passes_filter = False
                    if passes_filter: filtered_commit_objects.append(commit_obj)
            logger.info(f"Found {len(filtered_commit_objects)} commits after Python date filtering for {self.repo_name}. Processing stats...")
            
            for commit in tqdm(filtered_commit_objects, desc=f"Processing stats for {self.repo_name}"):
                author_name = self.author_resolver.get_canonical_name(commit.author.name, commit.author.email)
                try:
                    stats = commit.stats
                    added_lines = stats.total.get('insertions', 0)
                    deleted_lines = stats.total.get('deletions', 0)
                    files_changed_count = stats.total.get('files', 0)
                except Exception as e:
                    logger.warning(f"Could not get stats for commit {commit.hexsha} in {self.repo_name}: {e}")
                    added_lines, deleted_lines, files_changed_count = 0,0,0
                
                original_message = commit.message.strip()
                subject_line = original_message.split('\n', 1)[0]

                commits_data.append({
                    "hash": commit.hexsha, "author_name": author_name, "author_email": commit.author.email,
                    "date": commit.authored_datetime.isoformat(), "message": subject_line,
                    "added_lines": added_lines, "deleted_lines": deleted_lines,
                    "net_lines": added_lines - deleted_lines, "files_changed": files_changed_count,
                    "is_merge": len(commit.parents) > 1,
                })
        except Exception as e: logger.error(f"Error processing commits in {self.repo_name}: {e}", exc_info=True)
        return commits_data

# --- Analysis Functions ---
def analyze_commit_metrics(commits_list_of_dicts):
    if not commits_list_of_dicts: return {}
    metrics = {
        "total_commits": len(commits_list_of_dicts), "commits_per_author": Counter(),
        "loc_per_author": defaultdict(lambda: {"added": 0, "deleted": 0, "net": 0, "commits": 0}),
        "commits_per_week": Counter(), "loc_added_per_week": Counter(), "loc_deleted_per_week": Counter(),
        "total_added_lines": 0, "total_deleted_lines": 0, "total_net_lines": 0,
        "active_days_per_author": defaultdict(lambda: set()), "first_commit_date": None, "last_commit_date": None,
        "commit_message_lengths": [], "merge_commits_count": 0, "files_changed_per_commit_avg": 0,
        "churn_per_author": defaultdict(lambda: 0), "churn_per_week": Counter(),
    }
    parsed_commits_for_sorting = []
    for cd in commits_list_of_dicts:
        try:
            dt = date_parser.isoparse(cd['date'])
            parsed_commits_for_sorting.append({'original': cd, 'datetime': dt})
        except ValueError:
            logger.warning(f"Could not parse date string '{cd['date']}' for commit {cd.get('hash','N/A')}. Skipping this commit for date-based metrics.")
            continue # Skip commits with unparseable dates for metrics relying on date objects

    if not parsed_commits_for_sorting: return metrics # All dates might have been unparseable
    parsed_commits_for_sorting.sort(key=lambda x: x['datetime'])
    metrics["first_commit_date"] = parsed_commits_for_sorting[0]['datetime']
    metrics["last_commit_date"] = parsed_commits_for_sorting[-1]['datetime']

    for entry in parsed_commits_for_sorting:
        commit, commit_date_obj = entry['original'], entry['datetime']
        author, week_key = commit['author_name'], get_week_year(commit_date_obj)
        metrics["commits_per_author"][author] += 1
        loc_s = metrics["loc_per_author"][author]
        loc_s["added"] += commit['added_lines']; loc_s["deleted"] += commit['deleted_lines']
        loc_s["net"] += commit['net_lines']; loc_s["commits"] += 1
        metrics["commits_per_week"][week_key] += 1
        metrics["loc_added_per_week"][week_key] += commit['added_lines']
        metrics["loc_deleted_per_week"][week_key] += commit['deleted_lines']
        metrics["churn_per_week"][week_key] += commit['added_lines'] + commit['deleted_lines']
        metrics["total_added_lines"] += commit['added_lines']; metrics["total_deleted_lines"] += commit['deleted_lines']
        metrics["total_net_lines"] += commit['net_lines']
        metrics["active_days_per_author"][author].add(commit_date_obj.date())
        metrics["commit_message_lengths"].append(len(commit['message']))
        if commit['is_merge']: metrics['merge_commits_count'] +=1
        metrics["churn_per_author"][author] += commit['added_lines'] + commit['deleted_lines']

    if metrics["total_commits"] > 0:
        metrics["files_changed_per_commit_avg"] = sum(c['files_changed'] for c in commits_list_of_dicts) / metrics["total_commits"]
        if metrics["commit_message_lengths"]: metrics["avg_commit_message_length"] = sum(metrics["commit_message_lengths"]) / len(metrics["commit_message_lengths"])
        else: metrics["avg_commit_message_length"] = 0
    else: metrics["files_changed_per_commit_avg"] = 0; metrics["avg_commit_message_length"] = 0
    
    metrics["active_days_count_per_author"] = {a: len(d) for a, d in metrics["active_days_per_author"].items()}
    metrics["commits_per_author"] = dict(sorted(metrics["commits_per_author"].items(), key=lambda i: i[1], reverse=True))
    metrics["loc_per_author"] = dict(sorted(metrics["loc_per_author"].items(), key=lambda i: i[1]['commits'], reverse=True))
    metrics["churn_per_author"] = dict(sorted(metrics["churn_per_author"].items(), key=lambda i: i[1], reverse=True))
    for key in ["commits_per_week", "loc_added_per_week", "loc_deleted_per_week", "churn_per_week"]:
        metrics[key] = dict(sorted(metrics[key].items()))
    metrics["top_contributors_by_commits_list"] = list(metrics["commits_per_author"].items())[:10]
    metrics["top_contributors_by_net_loc_list"] = sorted(metrics["loc_per_author"].items(), key=lambda i: i[1]['net'], reverse=True)[:10]
    metrics["top_contributors_by_churn_list"] = list(metrics["churn_per_author"].items())[:10]
    return metrics

# --- Reporting Functions ---
def generate_console_report(repo_analysis_data_map, overall_summary_dict=None):
    if not tabulate:
        logger.warning("Tabulate library not found. Console reporting will be basic.")
        # Fallback to JSON print for data structure
        for repo_name_key, data_val in repo_analysis_data_map.items():
            print(f"\n--- Report for Repository: {repo_name_key} ---")
            if data_val and data_val.get("commit_metrics"): print(json.dumps(data_val["commit_metrics"], indent=2, default=str))
            else: print("No commit metrics data for this repository.")
        if overall_summary_dict: print("\n--- Overall Summary ---"); print(json.dumps(overall_summary_dict, indent=2, default=str))
        return

    for repo_name_key, data_val in repo_analysis_data_map.items():
        print(f"\n\n{'='*20} Report for Repository: {repo_name_key} {'='*20}")
        cm = data_val.get("commit_metrics", {})
        if not cm or not cm.get("total_commits"): print("No commit metrics data or zero commits for this repository."); continue

        print("\n--- Commit Summary ---")
        commit_summary_table_data = [
            ("Total Commits", f"{cm.get('total_commits', 0):,}" ), ("Total Added Lines", f"{cm.get('total_added_lines', 0):,}" ),
            ("Total Deleted Lines", f"{cm.get('total_deleted_lines', 0):,}" ), ("Total Net Lines", f"{cm.get('total_net_lines', 0):,}" ),
            ("First Commit Date", cm.get("first_commit_date", "N/A").strftime('%Y-%m-%d %H:%M %Z') if cm.get("first_commit_date") else "N/A"),
            ("Last Commit Date", cm.get("last_commit_date", "N/A").strftime('%Y-%m-%d %H:%M %Z') if cm.get("last_commit_date") else "N/A"),
            ("Avg Files Changed/Commit", f"{cm.get('files_changed_per_commit_avg', 0):.2f}"),
            ("Avg Commit Message Length", f"{cm.get('avg_commit_message_length', 0):.1f} chars"),
            ("Merge Commits", f"{cm.get('merge_commits_count', 0):,}" ),
        ]
        print(tabulate(commit_summary_table_data, headers=["Metric", "Value"], tablefmt="grid"))

        print("\n--- Top 5 Commit Authors (by Commits) ---")
        print(tabulate([(a, f"{s:,}") for a,s in cm.get("top_contributors_by_commits_list", [])[:5]], headers=["Author","Commits"], tablefmt="grid"))
        
        print("\n--- Top 5 Commit Authors (by Net LoC) ---")
        print(tabulate([[a,f"{s.get('net',0):,}",f"{s.get('added',0):,}",f"{s.get('deleted',0):,}",f"{s.get('commits',0):,}"] for a,s in cm.get("top_contributors_by_net_loc_list",[])[:5]], headers=["Author","Net LoC","Added","Deleted","Commits"], tablefmt="grid"))
        
        print("\n--- Top 5 Commit Authors (by Churn) ---")
        loc_map = cm.get('loc_per_author', {})
        churn_data = [[a, f"{ch_val:,}", f"{loc_map.get(a,{}).get('added',0):,}", f"{loc_map.get(a,{}).get('deleted',0):,}", f"{loc_map.get(a,{}).get('commits',0):,}"] for a,ch_val in cm.get("top_contributors_by_churn_list",[])[:5]]
        print(tabulate(churn_data, headers=["Author","Total Churn","Added","Deleted","Commits"], tablefmt="grid"))

        print("\n--- Commits per Week (Last 10 weeks or all if fewer) ---")
        print(tabulate([(w,f"{c:,}") for w,c in list(cm.get("commits_per_week",{}).items())[-10:]], headers=["Week","Commits"], tablefmt="grid"))
        
        print("\n--- Churn per Week (Last 10 weeks or all if fewer) ---")
        print(tabulate([(w,f"{c:,}") for w,c in list(cm.get("churn_per_week",{}).items())[-10:]], headers=["Week","Churn"], tablefmt="grid"))

    if overall_summary_dict:
        print(f"\n\n{'='*25} Overall Summary ({overall_summary_dict.get('total_repositories_with_metrics',0)} Repos with Metrics) {'='*25}")
        overall_table = [
            ("Total Repositories Analyzed", overall_summary_dict.get("total_repositories_analyzed", "N/A")),
            ("Total Repositories w/ Metrics", overall_summary_dict.get("total_repositories_with_metrics", "N/A")),
            ("Total Commits", f"{overall_summary_dict.get('grand_total_commits', 0):,}" ),
            ("Total LoC Added", f"{overall_summary_dict.get('grand_total_loc_added', 0):,}" ),
            ("Total LoC Deleted", f"{overall_summary_dict.get('grand_total_loc_deleted', 0):,}" ),
            ("Total Churn", f"{overall_summary_dict.get('grand_total_churn', 0):,}" ),
        ]
        print(tabulate(overall_table, headers=["Overall Metric", "Value"], tablefmt="grid"))
        print("\n--- Top 10 Contributors (Commits - All Repos) ---")
        print(tabulate([(a,f"{s:,}") for a,s in overall_summary_dict.get("overall_top_commit_contributors_list",[])[:10]], headers=["Author","Commits"], tablefmt="grid"))
        print("\n--- Top 10 Contributors (Net LoC - All Repos) ---")
        print(tabulate([[a,f"{d.get('net',0):,}",f"{d.get('added',0):,}",f"{d.get('deleted',0):,}",f"{d.get('commits',0):,}"] for a,d in overall_summary_dict.get("overall_top_loc_contributors_list",[])[:10]], headers=["Author","Net LoC","Added","Deleted","Commits"], tablefmt="grid"))
        print("\n--- Top 10 Contributors (Churn - All Repos) ---")
        loc_dict_all = overall_summary_dict.get("overall_loc_contributors_dict",{})
        churn_all_data = [[a, f"{ch_val:,}", f"{loc_dict_all.get(a,{}).get('added',0):,}", f"{loc_dict_all.get(a,{}).get('deleted',0):,}", f"{loc_dict_all.get(a,{}).get('commits',0):,}"] for a,ch_val in overall_summary_dict.get("overall_top_churn_contributors_list",[])[:10]]
        print(tabulate(churn_all_data, headers=["Author","Total Churn","Added","Deleted","Commits"], tablefmt="grid"))

def save_to_json(data_to_save, filepath):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            def dt_converter(o):
                if isinstance(o, datetime): return o.isoformat()
                raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
            json.dump(data_to_save, f, indent=2, default=dt_converter)
        logger.info(f"Successfully saved JSON report to {filepath}")
    except Exception as e: logger.error(f"Error saving JSON report to {filepath}: {e}")

def save_to_csv(repo_analysis_data_map, output_dir_path):
    if not pd: logger.warning("Pandas library not found. CSV reporting is skipped."); return
    all_commits_summary_list_for_df, all_detailed_commits_list_for_df = [], []
    for repo_name_key, data_val in repo_analysis_data_map.items():
        cm = data_val.get("commit_metrics", {})
        if cm and cm.get("total_commits", 0) > 0 : # Only create summary entry if there are commits
            all_commits_summary_list_for_df.append({
                "repository": repo_name_key, "total_commits": cm.get("total_commits"),
                "total_added_lines": cm.get("total_added_lines"), "total_deleted_lines": cm.get("total_deleted_lines"),
                "first_commit_date": cm.get("first_commit_date").isoformat() if cm.get("first_commit_date") else None,
                "last_commit_date": cm.get("last_commit_date").isoformat() if cm.get("last_commit_date") else None,
                "merge_commits": cm.get("merge_commits_count"),
            })
        if data_val.get("raw_commits"): # raw_commits now has subject line for "message"
            for commit_dict in data_val["raw_commits"]:
                aug_commit_dict = commit_dict.copy(); aug_commit_dict["repository"] = repo_name_key
                all_detailed_commits_list_for_df.append(aug_commit_dict)
    
    if all_commits_summary_list_for_df:
        summary_df = pd.DataFrame(all_commits_summary_list_for_df)
        s_path = os.path.join(output_dir_path, "summary_all_repos_commits.csv")
        try: summary_df.to_csv(s_path, index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL); logger.info(f"Saved summary CSV to {s_path}")
        except Exception as e: logger.error(f"Error saving summary CSV to {s_path}: {e}")
    else: logger.info("No summary data to write to summary_all_repos_commits.csv (e.g., no repos with commits found).")
    
    if all_detailed_commits_list_for_df:
        detailed_df = pd.DataFrame(all_detailed_commits_list_for_df)
        d_path = os.path.join(output_dir_path, "detailed_all_repos_commits.csv")
        try:
            # QUOTE_NONNUMERIC is generally good if numbers are clean and text fields might have delimiters
            # Subject lines should be relatively safe.
            detailed_df.to_csv(d_path, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
            logger.info(f"Saved detailed commits CSV to {d_path}")
        except Exception as e: logger.error(f"Error saving detailed CSV to {d_path}: {e}")
    else: logger.info("No detailed commit data to write to detailed_all_repos_commits.csv.")

def generate_markdown_report(repo_analysis_data_map, output_dir_path, overall_summary_dict=None):
    for repo_name_key, data_val in repo_analysis_data_map.items():
        cm = data_val.get("commit_metrics", {})
        md_content = f"# Analysis Report: {repo_name_key}\n\n"
        if not cm or not cm.get("total_commits"): md_content += "No commit data for this repository within the filter.\n"
        else:
            md_content += "## Summary\n"
            md_content += f"- Total Commits: {cm.get('total_commits', 0):,}\n"
            md_content += f"- LoC Added: {cm.get('total_added_lines', 0):,}, Deleted: {cm.get('total_deleted_lines', 0):,}, Net: {cm.get('total_net_lines', 0):,}\n"
            fc_date = cm.get("first_commit_date").strftime('%Y-%m-%d') if cm.get("first_commit_date") else "N/A"
            lc_date = cm.get("last_commit_date").strftime('%Y-%m-%d') if cm.get("last_commit_date") else "N/A"
            md_content += f"- Period: {fc_date} to {lc_date}\n"
            if tabulate:
                md_content += "\n### Top 3 Commit Authors (by Commits)\n"
                md_content += tabulate([(a,f"{c:,}") for a,c in cm.get("top_contributors_by_commits_list",[])[:3]], headers=["Author","Commits"],tablefmt="pipe") + "\n"
        md_path = os.path.join(output_dir_path, f"report_{repo_name_key}.md")
        try: open(md_path, 'w', encoding='utf-8').write(md_content)
        except Exception as e: logger.error(f"Error writing MD for {repo_name_key}: {e}")
    if overall_summary_dict and overall_summary_dict.get("total_repositories_with_metrics", 0) > 0:
        md_overall = f"# Overall Summary\n\n"
        md_overall += f"- Repositories with Metrics: {overall_summary_dict['total_repositories_with_metrics']:,} / {overall_summary_dict['total_repositories_analyzed']:,}\n"
        md_overall += f"- Total Commits: {overall_summary_dict['grand_total_commits']:,}\n"
        if tabulate:
             md_overall += "\n## Top 5 Overall Commit Authors\n"
             md_overall += tabulate([(a,f"{c:,}") for a,c in overall_summary_dict.get("overall_top_commit_contributors_list",[])[:5]], headers=["Author","Commits"],tablefmt="pipe") + "\n"
        overall_path = os.path.join(output_dir_path, "summary_overall_report.md")
        try: open(overall_path, 'w', encoding='utf-8').write(md_overall)
        except Exception as e: logger.error(f"Error writing overall MD: {e}")
    logger.info(f"Markdown reports generation attempt finished in {output_dir_path}")

# --- Main Orchestration ---
def analyze_single_repository_local(repo_disk_path, author_resolver, since_dt_obj, until_dt_obj):
    repo_name_basename = os.path.basename(repo_disk_path)
    logger.info(f"Analyzing repository: {repo_name_basename} (path: {repo_disk_path})")
    repo_data_result = {"repo_name": repo_name_basename, "commit_metrics": {}, "raw_commits": []}
    try:
        analyzer = LocalGitAnalyzer(repo_disk_path, author_resolver, since_dt_obj, until_dt_obj)
        commits_as_dicts = analyzer.get_commit_data()
        repo_data_result["raw_commits"] = commits_as_dicts
        if commits_as_dicts: repo_data_result["commit_metrics"] = analyze_commit_metrics(commits_as_dicts)
        else: logger.warning(f"No commits after filtering for {repo_disk_path}")
    except (InvalidGitRepositoryError, NoSuchPathError): logger.error(f"Skipping invalid/missing repo: {repo_disk_path}") ; return None
    except Exception as e: logger.error(f"Failed analysis for {repo_disk_path}: {e}", exc_info=True); return None
    return repo_data_result

def main():
    parser = argparse.ArgumentParser(description="Local Git Analyzer - Analyzes Git repositories from disk.")
    parser.add_argument("main_folder", help="Path to folder containing cloned Git repositories.")
    parser.add_argument("--output_dir", default="./local_git_analysis_reports", help="Directory for report files.")
    parser.add_argument("--formats", default="console,csv,json,md", help="Comma-separated output formats.")
    parser.add_argument("--since_date", help="Analyze commits since this date (YYYY-MM-DD). Inclusive.")
    parser.add_argument("--until_date", help="Analyze commits until this date (YYYY-MM-DD). Inclusive.")
    parser.add_argument("--max_workers", type=int, default=DEFAULT_MAX_WORKERS, help="Max parallel workers.")
    parser.add_argument("--log_level", default="INFO", choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'], help="Logging level.")
    parser.add_argument("--author_alias_file", help="Path to YAML file for author aliasing.")
    parser.add_argument("--repo_names", help="Comma-separated list of specific repository folder names to analyze.")
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger.info(f"Local Git Analyzer started. Output: '{args.output_dir}'. Log Level: {args.log_level}.")
    if not os.path.isdir(args.main_folder): logger.critical(f"Main folder not found: {args.main_folder}"); return
    try: os.makedirs(args.output_dir, exist_ok=True)
    except OSError as e: logger.critical(f"Cannot create output dir '{args.output_dir}': {e}"); return

    since_dt_obj = parse_iso_datetime_to_aware_utc(args.since_date, False) if args.since_date else None
    until_dt_obj = parse_iso_datetime_to_aware_utc(args.until_date, True) if args.until_date else None
    if (args.since_date and not since_dt_obj) or (args.until_date and not until_dt_obj):
        logger.critical("Exiting due to invalid date format(s)."); return

    author_resolver = AuthorAliasResolver(args.author_alias_file)
    target_repos = {n.strip() for n in args.repo_names.split(',')} if args.repo_names else None
    repo_paths = [os.path.join(args.main_folder, i) for i in os.listdir(args.main_folder)
                  if os.path.isdir(os.path.join(args.main_folder, i, '.git')) and
                     (not target_repos or i in target_repos)]
    
    if not repo_paths: logger.warning("No valid Git repositories found/specified."); return
    
    all_results_map = {}
    logger.info(f"Found {len(repo_paths)} repos. Analyzing with up to {args.max_workers} workers.")
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_map = {executor.submit(analyze_single_repository_local, p, author_resolver, since_dt_obj, until_dt_obj): p for p in repo_paths}
        for future in tqdm(as_completed(future_map), total=len(repo_paths), desc="Analyzing Repos"):
            path_key = future_map[future]; name_key = os.path.basename(path_key)
            try:
                res = future.result()
                if res: all_results_map[name_key] = res # Store result even if commit_metrics is empty
                else: logger.warning(f"No result returned for '{name_key}' (it might have failed processing).")
            except Exception as e: logger.error(f"Error processing result for {name_key}: {e}", exc_info=True)
    
    if not all_results_map: logger.error("No data collected from any repository. Exiting report generation."); return

    overall_summary = {
        "total_repositories_analyzed": len(all_results_map), "grand_total_commits": 0, "grand_total_loc_added": 0,
        "grand_total_loc_deleted": 0, "grand_total_churn": 0, "overall_commit_contributors": Counter(),
        "overall_loc_contributors_dict": defaultdict(lambda: {"added":0,"deleted":0,"net":0,"commits":0}),
        "overall_churn_contributors_dict": Counter(), "total_repositories_with_metrics": 0
    }
    for repo_name, repo_data in all_results_map.items():
        cm = repo_data.get("commit_metrics")
        if not cm or not cm.get("total_commits"): continue # Skip if no commit_metrics or no actual commits
        overall_summary["total_repositories_with_metrics"] += 1
        overall_summary["grand_total_commits"] += cm.get("total_commits", 0)
        overall_summary["grand_total_loc_added"] += cm.get("total_added_lines", 0)
        overall_summary["grand_total_loc_deleted"] += cm.get("total_deleted_lines", 0)
        overall_summary["grand_total_churn"] += cm.get("total_added_lines", 0) + cm.get("total_deleted_lines", 0)
        for auth, cnt in cm.get("commits_per_author", {}).items(): overall_summary["overall_commit_contributors"][auth] += cnt
        for auth, ldata in cm.get("loc_per_author", {}).items():
            loc_d = overall_summary["overall_loc_contributors_dict"][auth]
            for lk in ["added","deleted","net","commits"]: loc_d[lk] += ldata.get(lk,0)
        for auth, ch_val in cm.get("churn_per_author", {}).items(): overall_summary["overall_churn_contributors_dict"][auth] += ch_val
    
    overall_summary["overall_top_commit_contributors_list"] = overall_summary["overall_commit_contributors"].most_common()
    overall_summary["overall_top_loc_contributors_list"] = sorted(overall_summary["overall_loc_contributors_dict"].items(), key=lambda x: x[1]['net'], reverse=True)
    overall_summary["overall_top_churn_contributors_list"] = sorted(overall_summary["overall_churn_contributors_dict"].items(), key=lambda x:x[1], reverse=True)

    formats = [f.strip().lower() for f in args.formats.split(',')]
    if "console" in formats: generate_console_report(all_results_map, overall_summary)
    if "json" in formats:
        save_to_json(all_results_map, os.path.join(args.output_dir, "full_local_analysis_data_per_repo.json"))
        save_to_json(overall_summary, os.path.join(args.output_dir, "overall_local_summary_data.json"))
    if "csv" in formats:
        if pd: save_to_csv(all_results_map, args.output_dir)
        else: logger.warning("CSV format requested but pandas library is not available.")
    if "md" in formats: generate_markdown_report(all_results_map, args.output_dir, overall_summary)

    logger.info(f"Local Git Analyzer finished. Reports in '{os.path.abspath(args.output_dir)}'")

if __name__ == "__main__":
    main()