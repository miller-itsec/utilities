#!/usr/bin/env python3

import argparse
import os
import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime, timezone
import logging
import calendar # For day of week names
import base64 # For embedding images

# --- Library Import Handling ---
PLOTLY_INSTALLED = False
JINJA2_INSTALLED = False
KALEIDO_INSTALLED = False # Still needed by Plotly to render to bytes for base64

# Initialize imported names to None so they are defined even if import fails
go = None
px = None
get_plotlyjs = None # Kept for template compatibility, though less critical for pure base64 image reports
_Jinja2_Environment_class = None 
_FileSystemLoader_class = None
_select_autoescape_class = None

try:
    import plotly.graph_objects as go_module
    import plotly.express as px_module
    from plotly.offline import get_plotlyjs as get_plotlyjs_module 
    
    go, px, get_plotlyjs = go_module, px_module, get_plotlyjs_module
    PLOTLY_INSTALLED = True
    try:
        import kaleido 
        KALEIDO_INSTALLED = True
        # logger.info("Kaleido library found for static image export.") # Logger not defined yet
    except ImportError:
        print("WARNING: kaleido library not found. Static image chart export (and base64 embedding) will not be available. Install: pip install kaleido")
except ImportError:
    print("ERROR: plotly library not found. Visualizations will not be available. Install: pip install plotly")

try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    _Jinja2_Environment_class = Environment
    _FileSystemLoader_class = FileSystemLoader
    _select_autoescape_class = select_autoescape
    JINJA2_INSTALLED = True
except ImportError:
    print("ERROR: Jinja2 library not found. HTML reporting will not be available. Install: pip install Jinja2")

# --- Global Configuration & Constants ---
PLOTLY_JS_SOURCE_FOR_TEMPLATE = 'cdn' 
DEFAULT_TOP_N_CONTRIBUTORS = 15
DEFAULT_TOP_N_REPOS = 20

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger("GitVisualizerSingleFile")

# --- Enhanced HTML Template (for Base64 Embedded Images) ---
DEFAULT_HTML_EMBEDDED_IMAGE_TEMPLATE_STRING = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Git Analysis Visualization (Single File) - {{ report_generation_time }}</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"; margin: 0; padding: 0; background-color: #f4f7f9; color: #333; display: flex; font-size: 14px; line-height:1.6; }
        nav#sidebar { width: 260px; background-color: #2d3e50; color: #ecf0f1; padding: 20px; height: 100vh; position: fixed; overflow-y: auto; border-right: 1px solid #e0e0e0; box-sizing: border-box;}
        nav#sidebar h2 { color: #fff; margin-top:0; border-bottom: 1px solid #4a627a; padding-bottom: 15px; font-size: 1.4em;}
        nav#sidebar ul { list-style-type: none; padding: 0; margin-top: 15px;}
        nav#sidebar ul li a { color: #ced4da; text-decoration: none; display: block; padding: 10px 15px; border-radius: 5px; margin-bottom: 5px; transition: background-color 0.2s ease-in-out, color 0.2s ease-in-out; font-size:0.95em;}
        nav#sidebar ul li a:hover, nav#sidebar ul li a.active { background-color: #495057; color: #fff; }
        nav#sidebar ul li strong { color: #adb5bd; font-size: 0.9em; padding: 15px 15px 5px; display:block; }
        .main-content { margin-left: 280px; padding: 25px; width: calc(100% - 280px); box-sizing: border-box; }
        .container { max-width: 100%; margin: 0 auto; }
        h1 { color: #2c3e50; text-align: left; border-bottom: 3px solid #3498db; padding-bottom:15px; margin-top:0; margin-bottom:25px; font-size: 2em;}
        h2 { color: #34495e; margin-top: 30px; border-bottom: 1px solid #bdc3c7; padding-bottom:10px; font-size: 1.6em; }
        h3 { color: #566573; margin-top: 25px; margin-bottom: 15px; font-size: 1.3em; }
        .section { margin-bottom: 35px; padding: 25px; background-color: #fff; box-shadow: 0 4px 12px rgba(0,0,0,0.08); border-radius: 8px; }
        .grid-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); gap: 25px; }
        .chart-container { width: 100%; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.05); box-sizing: border-box; text-align: center;}
        .chart-container img { max-width: 100%; height: auto; border: 1px solid #e0e0e0; border-radius: 4px; margin-top:10px;}
        table.styled-table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 0.9em; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 6px; overflow:hidden; }
        table.styled-table thead tr { background-color: #3498db; color: #ffffff; text-align: left; font-weight: bold; }
        table.styled-table th, table.styled-table td { padding: 12px 15px; border: 1px solid #e0e0e0;}
        table.styled-table tbody tr { border-bottom: 1px solid #f0f0f0; }
        table.styled-table tbody tr:nth-of-type(even) { background-color: #f8f9fa; }
        table.styled-table tbody tr:last-of-type { border-bottom: 2px solid #3498db; }
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 20px; margin-bottom: 25px; }
        .kpi { background-color: #ffffff; padding: 20px; text-align: center; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.06); border: 1px solid #e9ecef;}
        .kpi .value { font-size: 2.2em; font-weight: 600; color: #3498db; line-height: 1.2;}
        .kpi .label { font-size: 0.95em; color: #555; margin-top: 8px; }
        .footer { text-align: center; margin-top: 40px; padding: 20px; font-size: 0.85em; color: #7f8c8d; border-top: 1px solid #e0e0e0;}
    </style>
</head>
<body>
    <nav id="sidebar">
        <h2>Git Visualization</h2>
        <ul>
            <li><a href="#overall-summary" class="active">Overall Summary</a></li>
            <li><hr style="border-color: #4a627a; margin: 10px 0;"></li>
            <li><strong>Repositories:</strong></li>
            {% for repo_name in repo_names %}
            <li><a href="#repo-{{ repo_name | replace('/', '-') | replace('.', '-') | replace('_', '-') }}">{{ repo_name }}</a></li>
            {% endfor %}
        </ul>
    </nav>

    <div class="main-content">
        <div class="container">
            <h1>Git Analysis Dashboard (Single File)</h1>
            <p style="margin-bottom:25px; font-size:0.9em; color: #555;">Generated on: {{ report_generation_time }} from CSV data located in: {{ csv_input_dir }}.</p>

            <div id="overall-summary" class="section">
                <h2>Overall Summary ({{ overall_summary_stats.total_repositories_analyzed }} Repositories)</h2>
                <div class="kpi-grid">
                    <div class="kpi"><div class="value">{{ "{:,.0f}".format(overall_summary_stats.grand_total_commits) if overall_summary_stats.grand_total_commits is not none else 'N/A' }}</div><div class="label">Total Commits</div></div>
                    <div class="kpi"><div class="value">{{ "{:,.0f}".format(overall_summary_stats.grand_total_loc_added) if overall_summary_stats.grand_total_loc_added is not none else 'N/A' }}</div><div class="label">Total Lines Added</div></div>
                    <div class="kpi"><div class="value">{{ "{:,.0f}".format(overall_summary_stats.grand_total_loc_deleted) if overall_summary_stats.grand_total_loc_deleted is not none else 'N/A' }}</div><div class="label">Total Lines Deleted</div></div>
                    <div class="kpi"><div class="value">{{ "{:,.0f}".format(overall_summary_stats.grand_total_churn) if overall_summary_stats.grand_total_churn is not none else 'N/A' }}</div><div class="label">Total Churn</div></div>
                </div>
                <div class="grid-container">
                    {% for chart_data_uri in overall_charts.values() %}
                        {% if chart_data_uri %}
                        <div class="chart-container"><img src="{{ chart_data_uri }}" alt="Overall Chart"/></div>
                        {% else %}
                        <div class="chart-container"><p>Chart could not be generated.</p></div>
                        {% endif %}
                    {% endfor %}
                </div>
            </div>

            {% for repo_name, data in repo_data.items() %}
            <div id="repo-{{ repo_name | replace('/', '-') | replace('.', '-') | replace('_', '-') }}" class="section">
                <h2>Repository: {{ repo_name }}</h2>
                {% if data.summary_metrics %}
                    <div class="kpi-grid">
                        <div class="kpi"><div class="value">{{ "{:,.0f}".format(data.summary_metrics.total_commits) if data.summary_metrics.total_commits is not none else 'N/A' }}</div><div class="label">Total Commits</div></div>
                        <div class="kpi"><div class="value">{{ "{:,.0f}".format(data.summary_metrics.total_added_lines) if data.summary_metrics.total_added_lines is not none else 'N/A' }}</div><div class="label">Lines Added</div></div>
                        <div class="kpi"><div class="value">{{ "{:,.0f}".format(data.summary_metrics.total_deleted_lines) if data.summary_metrics.total_deleted_lines is not none else 'N/A' }}</div><div class="label">Lines Deleted</div></div>
                        <div class="kpi"><div class="value">{{ data.summary_metrics.first_commit_date.strftime('%Y-%m-%d') if data.summary_metrics.first_commit_date else 'N/A' }}</div><div class="label">First Commit in Range</div></div>
                        <div class="kpi"><div class="value">{{ data.summary_metrics.last_commit_date.strftime('%Y-%m-%d') if data.summary_metrics.last_commit_date else 'N/A' }}</div><div class="label">Last Commit in Range</div></div>
                    </div>
                {% else %}
                    <p>No summary metrics available for this repository.</p>
                {% endif %}
                <div class="grid-container">
                    {% for chart_data_uri in data.charts.values() %}
                        {% if chart_data_uri %}
                        <div class="chart-container"><img src="{{ chart_data_uri }}" alt="Chart for {{ repo_name }}"/></div>
                        {% else %}
                        <div class="chart-container"><p>Chart for {{ repo_name }} could not be generated.</p></div>
                        {% endif %}
                    {% endfor %}
                </div>
                {% if data.tables.top_contributors_commits %}
                    <h3>Top Commit Contributors (Max {{ top_n_contributors }})</h3>
                    {{ data.tables.top_contributors_commits | safe }}
                {% endif %}
            </div>
            {% endfor %}
        </div>
        <div class="footer">
            Git Analysis Visualizer - Single File Edition
        </div>
    </div>
    <script> /* Basic sidebar active link JS - same as before */ </script>
</body>
</html>
"""

# --- Helper Functions ---
def setup_logging(log_level_str):
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', force=True)
    logger.setLevel(log_level)

def fig_to_base64_data_uri(fig, chart_name_for_log="chart"):
    """Converts a Plotly figure to a base64 data URI for embedding in HTML."""
    if not PLOTLY_INSTALLED or not KALEIDO_INSTALLED or fig is None:
        logger.warning(f"Cannot generate base64 for {chart_name_for_log} (Plotly/Kaleido not installed or no figure).")
        return None
    try:
        img_bytes = fig.to_image(format="png", scale=2) # scale for better resolution
        base64_string = base64.b64encode(img_bytes).decode('utf-8')
        data_uri = f"data:image/png;base64,{base64_string}"
        logger.debug(f"Successfully generated base64 data URI for {chart_name_for_log} (length: {len(data_uri)}).")
        return data_uri
    except Exception as e:
        logger.error(f"Failed to convert figure '{chart_name_for_log}' to base64 image: {e}", exc_info=True)
        return None

def safe_to_datetime(date_series):
    return pd.to_datetime(date_series, errors='coerce', utc=True)

# --- Plotting Functions WITH DEBUGGING (plot_commits_timeline, etc. from previous full script) ---
# These functions remain the same as they return 'fig' objects.
# The conversion to image/data_uri happens in generate_visualizations.
def plot_commits_timeline(df_commits, date_col='date', resample_freq='W', title="Commits Over Time"):
    if df_commits.empty or date_col not in df_commits.columns or not PLOTLY_INSTALLED or px is None:
        logger.warning(f"plot_commits_timeline [{title}]: Pre-check failed.")
        return None
    df_c = df_commits.copy(); df_c[date_col] = safe_to_datetime(df_c[date_col]); df_c = df_c.dropna(subset=[date_col])
    if df_c.empty: logger.warning(f"plot_commits_timeline [{title}]: DataFrame empty after date processing."); return None
    try: commits_over_time = df_c.set_index(date_col).resample(resample_freq).size().reset_index(name='commits')
    except Exception as e: logger.error(f"plot_commits_timeline [{title}]: Error during resampling: {e}", exc_info=True); return None
    logger.debug(f"DEBUG plot_commits_timeline [{title}, freq={resample_freq}]: Data for chart (head/tail):\n{commits_over_time.head().to_string()}\n...\n{commits_over_time.tail().to_string()}")
    if not commits_over_time.empty: logger.debug(f"DEBUG plot_commits_timeline [{title}, freq={resample_freq}]: Stats for 'commits':\n{commits_over_time['commits'].describe().to_string()}")
    else: logger.warning(f"plot_commits_timeline [{title}]: commits_over_time empty after resampling."); return None
    fig = px.line(commits_over_time, x=date_col, y='commits', title=title, markers=True, labels={date_col: "Date", "commits": "Commits"})
    fig.update_layout(margin=dict(l=40, r=20, t=60, b=40), title_x=0.5); return fig

def plot_loc_timeline(df_commits, date_col='date', resample_freq='W', title="Lines of Code (LoC) Over Time"):
    if df_commits.empty or date_col not in df_commits.columns or not PLOTLY_INSTALLED or go is None: return None
    df_l = df_commits.copy(); df_l[date_col] = safe_to_datetime(df_l[date_col]); df_l = df_l.dropna(subset=[date_col])
    if df_l.empty: return None
    try: loc_over_time = df_l.set_index(date_col).resample(resample_freq)[['added_lines', 'deleted_lines', 'net_lines']].sum().reset_index()
    except Exception as e: logger.error(f"plot_loc_timeline [{title}]: Error during resampling: {e}", exc_info=True); return None
    logger.debug(f"DEBUG plot_loc_timeline [{title}, freq={resample_freq}]: Data for chart (head/tail):\n{loc_over_time.head().to_string()}\n...\n{loc_over_time.tail().to_string()}")
    if loc_over_time.empty: logger.warning(f"plot_loc_timeline [{title}]: loc_over_time empty after resampling."); return None
    fig = go.Figure()
    fig.add_trace(go.Bar(x=loc_over_time[date_col], y=loc_over_time['added_lines'], name='Added', marker_color='mediumseagreen'))
    fig.add_trace(go.Bar(x=loc_over_time[date_col], y=-loc_over_time['deleted_lines'], name='Deleted', marker_color='indianred'))
    fig.add_trace(go.Scatter(x=loc_over_time[date_col], y=loc_over_time['net_lines'], name='Net Change', mode='lines+markers', line=dict(color='cornflowerblue')))
    fig.update_layout(barmode='relative', title_text=title, xaxis_title="Date", yaxis_title="Lines of Code", margin=dict(l=40,r=20,t=60,b=40), title_x=0.5, legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1)); return fig

def plot_top_contributors_bar(df_data, by_col='author_name', metric_col='commits', title_prefix="Top", top_n=10, is_overall=False):
    if df_data.empty or by_col not in df_data.columns or not PLOTLY_INSTALLED or px is None: logger.warning(f"plot_top_contributors_bar: Pre-check for {title_prefix} by {metric_col}"); return None
    df_d = df_data.copy()
    logger.debug(f"DEBUG plot_top_contributors_bar [{title_prefix}, metric={metric_col}, by={by_col}]: Input df_data shape: {df_d.shape}. Relevant cols head:\n{df_d[[by_col] + ([metric_col] if metric_col!='commits' and metric_col in df_d.columns else ([c for c in ['added_lines','deleted_lines'] if c in df_d.columns]))].head().to_string()}")
    if metric_col == 'commits':
        if not df_d.empty: agg_data = df_d.groupby(by_col, observed=True).size().reset_index(name='count')
        else: agg_data = pd.DataFrame(columns=[by_col, 'count'])
    elif metric_col in ['added_lines', 'deleted_lines', 'net_lines', 'churn']:
        if metric_col == 'churn' and 'churn' not in df_d.columns:
            if 'added_lines' in df_d.columns and 'deleted_lines' in df_d.columns: df_d['churn'] = df_d['added_lines'].fillna(0) + df_d['deleted_lines'].fillna(0)
            else: logger.warning(f"Cannot calc churn for {title_prefix}"); return None
        if metric_col not in df_d.columns: logger.warning(f"Metric col '{metric_col}' not found for {title_prefix}"); return None
        if not df_d.empty: agg_data = df_d.groupby(by_col, observed=True)[metric_col].sum().reset_index(name='count')
        else: agg_data = pd.DataFrame(columns=[by_col, 'count'])
    else: logger.warning(f"Unsupported metric_col '{metric_col}'"); return None
    if agg_data.empty: logger.warning(f"plot_top_contributors_bar [{title_prefix}, metric={metric_col}]: agg_data empty."); return None
    top_data = agg_data.sort_values(by='count', ascending=False).head(top_n)
    logger.debug(f"DEBUG plot_top_contributors_bar [{title_prefix}, metric={metric_col}]: top_data (final for chart):\n{top_data.to_string()}")
    if top_data.empty: return None
    title = f"{title_prefix} {top_n} Contributors by {metric_col.replace('_',' ').title()} {'Overall' if is_overall else ''}"
    fig = px.bar(top_data, y=by_col, x='count', title=title, orientation='h', labels={'count': metric_col.replace('_',' ').title(), by_col: 'Contributor'}, color='count', color_continuous_scale=px.colors.sequential.Blues)
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(l=180,r=20,t=60,b=40), title_x=0.5); return fig

def plot_overall_commits_per_repo(df_summary_commits, top_n=10):
    if df_summary_commits.empty or 'repository' not in df_summary_commits.columns or 'total_commits' not in df_summary_commits.columns or not PLOTLY_INSTALLED or px is None: logger.warning("plot_overall_commits_per_repo: Pre-check failed."); return None
    logger.debug(f"DEBUG plot_overall_commits_per_repo: Input df_summary_commits (first 5 of 'repository','total_commits'):\n{df_summary_commits[['repository', 'total_commits']].head().to_string()}")
    if not df_summary_commits['total_commits'].empty: logger.debug(f"DEBUG plot_overall_commits_per_repo: Stats for 'total_commits' in input:\n{df_summary_commits['total_commits'].describe().to_string()}")
    top_repos_for_chart = df_summary_commits.sort_values(by='total_commits', ascending=False).head(top_n)
    logger.debug(f"DEBUG plot_overall_commits_per_repo: top_repos_for_chart (data sent to chart):\n{top_repos_for_chart.to_string()}")
    if top_repos_for_chart.empty: logger.warning("plot_overall_commits_per_repo: top_repos_for_chart empty."); return None
    fig = px.bar(top_repos_for_chart, x='repository', y='total_commits', title=f"Top {top_n} Repositories by Total Commits", labels={'repository':'Repository','total_commits':'Total Commits'}, color='total_commits', color_continuous_scale=px.colors.sequential.Teal)
    fig.update_layout(margin=dict(l=40,r=20,t=60,b=120), title_x=0.5, xaxis_tickangle=-45); return fig

def plot_author_pie_chart(df_commits, author_col='author_name', top_n=10, title="Overall Commit Distribution by Author"):
    # This is the heavily debugged version from previous response
    if df_commits.empty or author_col not in df_commits.columns or not PLOTLY_INSTALLED or px is None: logger.warning(f"Pie chart [{title}]: Pre-check failed."); return None
    author_counts = df_commits[author_col].value_counts()
    logger.debug(f"DEBUG Pie Chart [{title}] - Initial author_counts (raw value_counts(), type: {type(author_counts)}):\n{author_counts.head(top_n + 5).to_string()}")
    plot_data_series = author_counts.nlargest(top_n).copy()
    if len(author_counts) > top_n:
        others_sum = author_counts.iloc[top_n:].sum() 
        if others_sum > 0:
            others_label = "Others (Aggregated)"; others_s = pd.Series([others_sum], index=[others_label])
            if others_label in plot_data_series.index: plot_data_series[others_label] += others_sum
            else: plot_data_series = pd.concat([plot_data_series, others_s])
    logger.debug(f"DEBUG Pie Chart [{title}] - plot_data_series after 'Others' (type: {type(plot_data_series)}):\n{plot_data_series.to_string()}")
    if plot_data_series.empty or plot_data_series.sum() == 0: logger.warning(f"Pie chart [{title}]: No data or sum is 0."); return None
    pie_df = plot_data_series.reset_index(); pie_df.columns = ['Author', 'Commits']
    pie_df['Commits'] = pd.to_numeric(pie_df['Commits'], errors='coerce').fillna(0)
    pie_df_final_plot = pie_df[pie_df['Commits'] > 0].copy()
    logger.debug(f"DEBUG Pie Chart [{title}] - FINAL pie_df_final_plot (Commits > 0) for px.pie:\n{pie_df_final_plot.to_string()}")
    logger.debug(f"DEBUG Pie Chart [{title}] - Dtypes of pie_df_final_plot: \n{pie_df_final_plot.dtypes.to_string()}")
    logger.debug(f"DEBUG Pie Chart [{title}] - Is 'Commits' numeric? {pd.api.types.is_numeric_dtype(pie_df_final_plot['Commits'])}")
    if pie_df_final_plot.empty: logger.warning(f"Pie chart [{title}]: pie_df_final_plot empty after Commits > 0 filter."); return None
    if pie_df_final_plot['Commits'].nunique()==1 and len(pie_df_final_plot)>1: logger.warning(f"DEBUG Pie Chart [{title}]: All commit values identical in pie_df_final_plot ({pie_df_final_plot['Commits'].iloc[0]}).")
    try:
        author_names_list = pie_df_final_plot['Author'].tolist(); commit_values_list = pie_df_final_plot['Commits'].tolist()
        logger.info(f"SUPER DEBUG Pie Chart [{title}] - Names List (len {len(author_names_list)}): {author_names_list}")
        logger.info(f"SUPER DEBUG Pie Chart [{title}] - Values List (len {len(commit_values_list)}): {commit_values_list}")
        for i, val in enumerate(commit_values_list):
            if not isinstance(val, (int, float, pd.Int64Dtype)): 
                logger.error(f"SUPER DEBUG Pie Chart [{title}]: Non-numeric value in values list at index {i}: '{val}' (type: {type(val)})")
                try: commit_values_list[i] = pd.to_numeric(val)
                except: logger.error(f"SUPER DEBUG: Failed to coerce {val} to numeric again.")
        if not author_names_list or not commit_values_list: logger.error(f"SUPER DEBUG Pie Chart [{title}]: Names or values list empty!"); return None
        fig = px.pie(names=author_names_list, values=commit_values_list, title=title, hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label', hovertemplate="Author: %{label}<br>Commits: %{value}<br>Percentage: %{percent}<extra></extra>")
        fig.update_layout(margin=dict(l=20,r=20,t=60,b=20), title_x=0.5, legend_title_text='Authors')
        logger.info(f"SUPER DEBUG Pie Chart [{title}]: Figure object created successfully.")
        return fig
    except Exception as e: logger.error(f"Error during SUPER DIRECT px.pie call for [{title}]: {e}", exc_info=True); return None

def plot_commit_heatmap(df_commits, date_col='date', title="Commit Activity Heatmap"):
    if df_commits.empty or date_col not in df_commits.columns or not PLOTLY_INSTALLED or go is None: return None
    df_h = df_commits.copy(); df_h[date_col] = safe_to_datetime(df_h[date_col]); df_h = df_h.dropna(subset=[date_col])
    if df_h.empty: return None
    df_h['day_of_week_num'] = df_h[date_col].dt.dayofweek; df_h['day_of_week_name'] = df_h[date_col].dt.day_name()
    df_h['hour_of_day'] = df_h[date_col].dt.hour
    try:
        heatmap_data = df_h.groupby(['day_of_week_num', 'day_of_week_name', 'hour_of_day']).size().reset_index(name='commits')
        heatmap_pivot = heatmap_data.pivot_table(index=['day_of_week_num','day_of_week_name'], columns='hour_of_day', values='commits', fill_value=0)
    except Exception as e: logger.error(f"Error creating pivot for heatmap [{title}]: {e}"); return None
    for hour in range(24):
        if hour not in heatmap_pivot.columns: heatmap_pivot[hour] = 0
    heatmap_pivot = heatmap_pivot.sort_index(level='day_of_week_num').reindex(columns=sorted(heatmap_pivot.columns))
    logger.debug(f"DEBUG plot_commit_heatmap [{title}]: Pivot table head:\n{heatmap_pivot.head().to_string()}")
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_pivot.values, x=[f"{h:02d}:00" for h in heatmap_pivot.columns], y=heatmap_pivot.index.get_level_values('day_of_week_name'),
        colorscale='Blues', hovertemplate="Day: %{y}<br>Hour: %{x}<br>Commits: %{z}<extra></extra>"
    ))
    fig.update_layout(title=title, xaxis_title="Hour of Day (UTC)", yaxis_title="Day of Week", margin=dict(l=100,r=20,t=60,b=40), title_x=0.5); return fig

def dataframe_to_html_table(df, columns=None, header=None, top_n=None):
    if df is None or df.empty: return "<p>No data available for this table.</p>"
    df_display = df.copy(); 
    if columns: existing_cols = [col for col in columns if col in df_display.columns]; df_display = df_display[existing_cols]
    if header and len(header) == len(df_display.columns): df_display.columns = header
    if top_n: df_display = df_display.head(top_n)
    return df_display.to_html(classes='styled-table', index=False, border=0, na_rep='N/A', escape=False)

# --- Main Processing ---
def generate_visualizations(csv_dir, output_html_file, template_path=None, top_n_contrib=DEFAULT_TOP_N_CONTRIBUTORS, top_n_repo=DEFAULT_TOP_N_REPOS):
    if not (PLOTLY_INSTALLED and JINJA2_INSTALLED and _Jinja2_Environment_class):
        logger.critical("Plotly or Jinja2 not available. Cannot generate HTML report."); return
    try:
        summary_path = os.path.join(csv_dir, "summary_all_repos_commits.csv"); detailed_path = os.path.join(csv_dir, "detailed_all_repos_commits.csv")
        if not (os.path.exists(summary_path) and os.path.exists(detailed_path)):
            logger.error(f"Required CSVs not found in '{csv_dir}'. Summary: {summary_path}, Detailed: {detailed_path}."); return
        df_summary_commits = pd.read_csv(summary_path); df_detailed_commits = pd.read_csv(detailed_path)
        logger.info(f"Loaded CSVs. Summary: {df_summary_commits.shape[0]} rows, Detailed: {df_detailed_commits.shape[0]} rows.")
        # Initial data debugging
        logger.debug("--- Initial df_detailed_commits Info ---"); df_detailed_commits.info(verbose=True, show_counts=True)
        logger.debug("--- Initial df_summary_commits Info ---"); df_summary_commits.info(verbose=True, show_counts=True)
        df_detailed_commits['date'] = safe_to_datetime(df_detailed_commits['date'])
        df_summary_commits['first_commit_date'] = safe_to_datetime(df_summary_commits['first_commit_date'])
        df_summary_commits['last_commit_date'] = safe_to_datetime(df_summary_commits['last_commit_date'])
        logger.debug("--- df_detailed_commits Info After Date Conversion ---"); df_detailed_commits.info(verbose=True, show_counts=True)
        logger.debug(f"NaNs in detailed 'date' after conversion: {df_detailed_commits['date'].isna().sum()}")
        logger.debug("--- df_summary_commits Info After Date Conversion ---"); df_summary_commits.info(verbose=True, show_counts=True)
    except Exception as e: logger.error(f"Error loading CSV data: {e}", exc_info=True); return

    loader = _FileSystemLoader_class(os.path.dirname(template_path)) if template_path and os.path.exists(template_path) and _FileSystemLoader_class else None
    if loader: logger.info(f"Using template file: {template_path}")
    else: logger.info("Using embedded HTML template string.")
    jinja_env_args = {'loader': loader}; autoescape_val = _select_autoescape_class(['html', 'xml']) if _select_autoescape_class else True
    jinja_env_args['autoescape'] = autoescape_val; env = _Jinja2_Environment_class(**jinja_env_args)
    template_source = DEFAULT_HTML_EMBEDDED_IMAGE_TEMPLATE_STRING # Use the correct template
    if loader and template_path:
        try: template = env.get_template(os.path.basename(template_path))
        except Exception as e: logger.error(f"Failed to load template file '{template_path}': {e}. Falling back to embedded."); template = env.from_string(template_source)
    else: template = env.from_string(template_source)
    
    report_render_data = {
        "report_generation_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "csv_input_dir": os.path.abspath(csv_dir),
        "repo_names": sorted(list(df_detailed_commits['repository'].astype(str).unique())),
        "repo_data": defaultdict(lambda: {"charts": {}, "tables": {}, "summary_metrics":{}}),
        "overall_summary_stats": {}, "overall_charts": {}, "top_n_contributors": top_n_contrib,
        "PLOTLY_INSTALLED": PLOTLY_INSTALLED, "plotly_js_source": PLOTLY_JS_SOURCE_FOR_TEMPLATE 
    }
    # No need for plotly_js_script if we are only embedding images
    # if PLOTLY_JS_SOURCE_FOR_TEMPLATE == 'embedded' and PLOTLY_INSTALLED and get_plotlyjs: 
    #     report_render_data["plotly_js_script"] = get_plotlyjs()

    oss = report_render_data["overall_summary_stats"]
    oss["total_repositories_analyzed"] = df_summary_commits['repository'].nunique()
    oss["grand_total_commits"] = df_detailed_commits.shape[0] if 'repository' in df_detailed_commits.columns else 0
    oss["grand_total_loc_added"] = df_detailed_commits['added_lines'].sum(skipna=True) if 'added_lines' in df_detailed_commits.columns else 0
    oss["grand_total_loc_deleted"] = df_detailed_commits['deleted_lines'].sum(skipna=True) if 'deleted_lines' in df_detailed_commits.columns else 0
    oss["grand_total_churn"] = oss["grand_total_loc_added"] + oss["grand_total_loc_deleted"]

    if PLOTLY_INSTALLED and KALEIDO_INSTALLED:
        report_render_data["overall_charts"]["commits_per_repo_png"] = fig_to_base64_data_uri(plot_overall_commits_per_repo(df_summary_commits, top_n=top_n_repo), "overall_commits_per_repo")
        report_render_data["overall_charts"]["overall_commits_weekly_png"] = fig_to_base64_data_uri(plot_commits_timeline(df_detailed_commits, title="Overall Commits (Weekly)", resample_freq='W'), "overall_commits_weekly")
        report_render_data["overall_charts"]["overall_commits_monthly_png"] = fig_to_base64_data_uri(plot_commits_timeline(df_detailed_commits, title="Overall Commits (Monthly)", resample_freq='ME'), "overall_commits_monthly")
        report_render_data["overall_charts"]["overall_loc_timeline_png"] = fig_to_base64_data_uri(plot_loc_timeline(df_detailed_commits, title="Overall LoC Changes"), "overall_loc_timeline")
        report_render_data["overall_charts"]["top_contrib_commits_png"] = fig_to_base64_data_uri(plot_top_contributors_bar(df_detailed_commits, metric_col='commits', top_n=top_n_contrib, is_overall=True), "overall_top_contrib_commits")
        report_render_data["overall_charts"]["top_contrib_netloc_png"] = fig_to_base64_data_uri(plot_top_contributors_bar(df_detailed_commits, metric_col='net_lines', top_n=top_n_contrib, is_overall=True), "overall_top_contrib_netloc")
        report_render_data["overall_charts"]["overall_author_pie_png"] = fig_to_base64_data_uri(plot_author_pie_chart(df_detailed_commits, top_n=top_n_contrib), "overall_author_pie")
        report_render_data["overall_charts"]["overall_commit_heatmap_png"] = fig_to_base64_data_uri(plot_commit_heatmap(df_detailed_commits, title="Overall Commit Activity Heatmap"), "overall_commit_heatmap")
    
    for repo_name in report_render_data["repo_names"]:
        logger.debug(f"Processing base64 charts for repo: {repo_name}")
        df_repo_commits = df_detailed_commits[df_detailed_commits['repository'] == repo_name].copy()
        repo_summary_s = df_summary_commits[df_summary_commits['repository'] == repo_name]
        repo_summary_info = repo_summary_s.iloc[0] if not repo_summary_s.empty else None
        current_repo_data = report_render_data["repo_data"][repo_name]
        if repo_summary_info is not None: current_repo_data["summary_metrics"] = {k: repo_summary_info.get(k) for k in ["total_commits","total_added_lines","total_deleted_lines","first_commit_date","last_commit_date"]}
        if PLOTLY_INSTALLED and KALEIDO_INSTALLED:
            repo_name_safe = "".join(c if c.isalnum() else "_" for c in repo_name)
            current_repo_data["charts"]["commits_timeline_png"] = fig_to_base64_data_uri(plot_commits_timeline(df_repo_commits, title=f"Commits Over Time"), f"{repo_name_safe}_commits_timeline")
            current_repo_data["charts"]["loc_timeline_png"] = fig_to_base64_data_uri(plot_loc_timeline(df_repo_commits, title=f"LoC Changes Over Time"), f"{repo_name_safe}_loc_timeline")
            current_repo_data["charts"]["top_contributors_commits_png"] = fig_to_base64_data_uri(plot_top_contributors_bar(df_repo_commits, metric_col='commits', top_n=top_n_contrib), f"{repo_name_safe}_top_contrib")
            current_repo_data["charts"]["commit_heatmap_png"] = fig_to_base64_data_uri(plot_commit_heatmap(df_repo_commits, title=f"Commit Activity Heatmap"), f"{repo_name_safe}_heatmap")
        if not df_repo_commits.empty and 'author_name' in df_repo_commits.columns:
            top_committers_repo = df_repo_commits.groupby('author_name').size().reset_index(name='commits').sort_values(by='commits', ascending=False)
            current_repo_data["tables"]["top_contributors_commits"] = dataframe_to_html_table(top_committers_repo, columns=['author_name', 'commits'], header=['Author', 'Commits'], top_n=top_n_contrib)
            
    html_content = template.render(report_render_data)
    try:
        with open(output_html_file, 'w', encoding='utf-8') as f: f.write(html_content)
        logger.info(f"Successfully generated single-file HTML report with embedded images: {output_html_file}")
    except Exception as e: logger.error(f"Error writing HTML report: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Generate HTML visualizations (embedded static images) from Git analysis CSVs.")
    parser.add_argument("csv_input_dir", help="Path to directory containing CSV files from analyze.py.")
    parser.add_argument("--output_file", default="./git_embedded_image_report.html", help="Path for the single HTML report file.") # Changed default name
    parser.add_argument("--template_file", help="Path to a custom Jinja2 HTML template file.")
    parser.add_argument("--log_level", default="INFO", choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'], help="Logging level.")
    parser.add_argument("--top_n_contributors", type=int, default=DEFAULT_TOP_N_CONTRIBUTORS, help="Number of top contributors.")
    parser.add_argument("--top_n_repos", type=int, default=DEFAULT_TOP_N_REPOS, help="Number of top repositories.")
    args = parser.parse_args()

    setup_logging(args.log_level)
    if not (PLOTLY_INSTALLED and JINJA2_INSTALLED): logger.critical("Plotly or Jinja2 missing. Exiting."); return
    if not KALEIDO_INSTALLED: 
        logger.critical("Kaleido not installed; cannot generate embedded image charts. Exiting.")
        return # Exit if Kaleido is essential for this mode

    logger.info(f"Starting single-file HTML visualization generation from CSVs in: {args.csv_input_dir}")
    generate_visualizations(args.csv_input_dir, args.output_file, args.template_file, args.top_n_contributors, args.top_n_repos)
    logger.info("Single-file HTML visualization generation process finished.")

if __name__ == "__main__":
    main()
