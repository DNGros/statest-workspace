"""Create a responsive bar plot of president surnames in street names."""

from pathlib import Path
import polars as pl

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# Always import matplotlib for color maps
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
matplotlib.use('Agg')  # Non-interactive backend

# Import president order for chronological sorting
from workspace.explore.president_surnames.presidents import PRESIDENT_SURNAMES


def create_grid_plot_html(df):
    """Create a responsive grid layout with pure CSS/HTML bars for each president."""
    # Sort by chronological order (president order)
    president_order = {name: idx for idx, name in enumerate(PRESIDENT_SURNAMES)}
    df_sorted = df.sort(
        pl.col("president_surname").map_elements(
            lambda x: president_order.get(x, 999), return_dtype=pl.Int64
        )
    )
    total_streets = df_sorted["street_count"].sum()
    
    # Find max count for consistent bar scaling
    max_count = df_sorted["street_count"].max()
    
    # Prepare data for HTML rendering
    president_data = []
    for row in df_sorted.iter_rows(named=True):
        surname = row["president_surname"]
        count = row["street_count"]
        pct = (count / total_streets * 100) if total_streets > 0 else 0
        bar_width_pct = (count / max_count * 100) if max_count > 0 else 0
        
        president_data.append({
            'surname': surname,
            'count': count,
            'pct': pct,
            'bar_width_pct': bar_width_pct,
        })
    
    # Create minimal HTML with just bars
    html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: sans-serif;
        }}
        
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 20px;
        }}
        
        .item {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .name {{
            min-width: 100px;
            font-size: 14px;
        }}
        
        .bar {{
            flex: 1;
            height: 20px;
            background: #ccc;
        }}
        
        .bar-fill {{
            height: 100%;
            background: #333;
        }}
        
        .value {{
            min-width: 60px;
            font-size: 14px;
            text-align: right;
        }}
        
        @media (max-width: 600px) {{
            .grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="grid">
"""
    
    # Add each president with minimal bar
    for data in president_data:
        html_template += f"""
        <div class="item">
            <div class="name">{data['surname']}</div>
            <div class="bar">
                <div class="bar-fill" style="width: {data['bar_width_pct']:.1f}%;"></div>
            </div>
            <div class="value">{data['count']:,}</div>
        </div>
"""
    
    html_template += """
    </div>
</body>
</html>
"""
    
    return html_template


def create_plotly_plot(df):
    """Create a simple single-chart plot (kept for backward compatibility)."""
    # Sort by chronological order
    president_order = {name: idx for idx, name in enumerate(PRESIDENT_SURNAMES)}
    df_sorted = df.sort(
        pl.col("president_surname").map_elements(
            lambda x: president_order.get(x, 999), return_dtype=pl.Int64
        )
    )
    total_streets = df_sorted["street_count"].sum()
    
    # Create vertical bar chart (bars going up)
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_sorted["president_surname"].to_list(),
        y=df_sorted["street_count"].to_list(),
        marker=dict(
            color=df_sorted["street_count"].to_list(),
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Streets")
        ),
        text=[f"{count:,}" for count in df_sorted["street_count"].to_list()],
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>%{y:,} streets<extra></extra>',
    ))
    
    fig.update_layout(
        title={
            'text': 'US President Surnames in Street Names<br><sub style="font-size:0.6em">Total: {:,} streets • Sorted chronologically</sub>'.format(total_streets),
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20},
        },
        xaxis_title='President (Chronological Order)',
        yaxis_title='Number of Streets',
        height=600,
        margin=dict(l=60, r=40, t=100, b=150),  # Extra bottom margin for rotated labels
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(size=11, family='Arial, sans-serif'),
        xaxis=dict(
            tickangle=-45,
            showgrid=False,
            tickfont=dict(size=9),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(0,0,0,0.1)',
            tickformat=',',
        ),
    )
    
    config = {
        'responsive': True,
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['pan2d', 'lasso2d', 'select2d'],
    }
    
    return fig, config


def create_matplotlib_plot(df):
    """Create a matplotlib plot as fallback."""
    # Sort by count descending
    df_sorted = df.sort("street_count", descending=True)
    
    # Create figure with appropriate size
    fig, ax = plt.subplots(figsize=(10, max(12, len(df_sorted) * 0.4)))
    
    # Create horizontal bar chart
    bars = ax.barh(
        df_sorted["president_surname"].to_list(),
        df_sorted["street_count"].to_list(),
        color=plt.cm.viridis(np.linspace(0, 1, len(df_sorted))[::-1])
    )
    
    # Add value labels on bars
    for i, (bar, count) in enumerate(zip(bars, df_sorted["street_count"].to_list())):
        width = bar.get_width()
        ax.text(width + 50, bar.get_y() + bar.get_height()/2, 
                f'{count:,}', ha='left', va='center', fontsize=9)
    
    ax.set_xlabel('Number of Streets', fontsize=12)
    ax.set_ylabel('President Surname', fontsize=12)
    ax.set_title('US President Surnames in Street Names', fontsize=16, pad=20)
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    
    # Format x-axis with commas
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    plt.tight_layout()
    return fig


def main():
    # Load data
    results_dir = Path(__file__).parent
    df = pl.read_csv(results_dir / "president_streets_overall.csv")
    
    print(f"Loaded data for {len(df)} presidents")
    print(f"Total streets: {df['street_count'].sum():,}")
    
    # Create plot
    if PLOTLY_AVAILABLE:
        print("Creating responsive grid plot (wraps into columns)...")
        html_content = create_grid_plot_html(df)
        
        # Save grid HTML
        output_path = results_dir / "president_streets_plot.html"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"✅ Saved responsive grid plot to: {output_path}")
        print(f"   Open in browser to view (automatically wraps into columns)")
        
        # Also create a simple single-chart version
        print("\nCreating single-chart version...")
        fig, config = create_plotly_plot(df)
        single_chart_path = results_dir / "president_streets_plot_single.html"
        fig.write_html(
            str(single_chart_path),
            config=config,
            include_plotlyjs='cdn',
        )
        print(f"✅ Saved single-chart version to: {single_chart_path}")
        
        # Also save as static PNG for reference
        try:
            png_path = results_dir / "president_streets_plot.png"
            fig.write_image(str(png_path), width=1400, height=600, scale=2)
            print(f"✅ Saved static PNG to: {png_path}")
        except Exception as e:
            print(f"⚠️  Could not save PNG (requires kaleido): {e}")
            print("   Install with: pip install kaleido")
    
    else:
        print("Creating matplotlib plot...")
        import numpy as np
        fig = create_matplotlib_plot(df)
        
        # Save as PNG
        png_path = results_dir / "president_streets_plot.png"
        fig.savefig(png_path, dpi=150, bbox_inches='tight')
        print(f"✅ Saved plot to: {png_path}")
        
        # Also try to save as HTML with matplotlib
        html_path = results_dir / "president_streets_plot.html"
        fig.savefig(html_path, format='svg', bbox_inches='tight')
        print(f"✅ Saved SVG to: {html_path}")


if __name__ == "__main__":
    main()

