#!/usr/bin/env python3
"""Generate an interactive choropleth map of state ego/humble rankings.

Creates a Plotly map colored by average rank (lower = more egotistical),
with detailed hover information showing all metrics.
"""

import sys
from pathlib import Path
from typing import Optional
import polars as pl
import plotly.graph_objects as go

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.explore.state_sts.combined_metrics_table import generate_combined_metrics_table
from workspace.plot_utils import get_output_path_from_script

# Mapping of state names to USPS abbreviations
STATE_ABBREV = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY', 'District Of Columbia': 'DC'
}


def create_state_rankings_map(output_path: Optional[Path] = None) -> go.Figure:
    """
    Create an interactive choropleth map of state rankings.
    
    Args:
        output_path: Optional path to save HTML map. If None, saves to main_outputs/mapping/
        
    Returns:
        Plotly Figure object
    """
    print("Generating combined metrics table...")
    df = generate_combined_metrics_table(output_path=None)
    
    # Add state abbreviations
    df = df.with_columns(
        pl.col("state_name").map_elements(
            lambda x: STATE_ABBREV.get(x, None),
            return_dtype=pl.Utf8
        ).alias("state_abbrev")
    )
    
    # Filter out states without abbreviations (shouldn't happen, but just in case)
    df = df.filter(pl.col("state_abbrev").is_not_null())
    
    # Convert to pandas for Plotly (it works better with pandas)
    df_pd = df.to_pandas()
    
    # Create hover text with all the details
    hover_text = []
    for _, row in df_pd.iterrows():
        text = (
            f"<b>{row['state_name']}</b><br>"
            f"<br>"
            f"<b>Average Rank: {row['avg_rank']:.1f}</b><br>"
            f"<br>"
            f"In-State %: {row['in_state_pct']:.1f}% (Rank: {int(row['rank_in_state'])})<br>"
            f"State Fraction: {row['state_fraction_pct']:.3f}% (Rank: {int(row['rank_state_fraction'])})<br>"
            f"Self-Named: {row['self_named_fraction']*100:.1f}% (Rank: {int(row['rank_self_named'])})"
        )
        hover_text.append(text)
    
    df_pd['hover_text'] = hover_text
    
    # Create the choropleth map
    # Lower avg_rank = more egotistical (warm yellow-gold)
    # Higher avg_rank = more humble (cool green-teal)
    # Diverging color scale with neutral midpoint
    
    # Choose color scale option:
    # 'custom' - Gold -> Beige -> Teal (matches your bar chart aesthetic)
    # 'BrBG' - Brown -> Beige -> Blue-Green (ColorBrewer, professional)
    # 'PiYG' - Pink -> White -> Yellow-Green (ColorBrewer, high contrast)
    # 'RdYlGn' - Red -> Yellow -> Green (ColorBrewer, intuitive red=bad, green=good)
    colorscale_option = 'BrBG'
    
    if colorscale_option == 'custom':
        colorscale = [
            [0.0, '#b8891c'],  # Vibrant gold (most egotistical)
            [0.5, '#E8E3D3'],  # Neutral beige (middle)
            [1.0, '#82c2a4']   # Strong teal (most humble)
        ]
    elif colorscale_option == 'BrBG':
        colorscale = 'BrBG'  # Brown-Beige-BlueGreen
    elif colorscale_option == 'PiYG':
        colorscale = 'PiYG'  # Pink-White-YellowGreen
    elif colorscale_option == 'RdYlGn':
        colorscale = 'RdYlGn'  # Red-Yellow-Green
    
    fig = go.Figure(data=go.Choropleth(
        locations=df_pd['state_abbrev'],
        z=df_pd['avg_rank'],
        locationmode='USA-states',
        colorscale=colorscale,
        showscale=True,  # Show colorbar
        colorbar=dict(
            title=None,
            tickmode='array',
            tickvals=[df_pd['avg_rank'].min(), df_pd['avg_rank'].max()],
            ticktext=['Uses Own Name More', 'Uses Own Name Less'],
            len=0.4,
            thickness=12,
            x=0.5,
            xanchor='center',
            y=0,
            yanchor='top',
            ypad=0,
            orientation='h',
            bgcolor='rgba(0,0,0,0)',  # Transparent colorbar background
        ),
        text=df_pd['hover_text'],
        hovertemplate='%{text}<extra></extra>',
        marker_line_color='white',
        marker_line_width=1.5,
    ))
    
    fig.update_layout(
        title=None,  # Remove title
        geo=dict(
            scope='usa',
            projection=go.layout.geo.Projection(type='albers usa'),
            showlakes=True,
            lakecolor='rgba(0,0,0,0)',  # Transparent lakes
            bgcolor='rgba(0,0,0,0)',  # Transparent map background
        ),
        # Reduced bottom margin since colorbar is closer
        margin=dict(l=0, r=0, t=5, b=0),  # Minimal margins, tight space for colorbar
        paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
        plot_bgcolor='rgba(0,0,0,0)',   # Transparent plot area
    )
    
    # Determine output path
    if output_path is None:
        output_path = get_output_path_from_script(
            Path(__file__),
            "state_rankings_map.html"
        )
    
    # Save HTML map with config to hide UI elements
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Disable drag mode in layout
    fig.update_layout(dragmode=False)
    
    # Generate HTML using Plotly's default full_html=True for simplicity
    # This creates a standard HTML document that works well in iframes
    full_html = fig.to_html(
        config={
            'displayModeBar': False,  # Hide the modebar (toolbar)
            'staticPlot': False,  # Keep hover interactions
            'scrollZoom': False,  # Disable scroll zoom
            'doubleClick': False,  # Disable double-click zoom
            'dragMode': False,  # Disable drag/pan
            'responsive': True,  # Make responsive
        },
        include_plotlyjs='cdn',  # Use CDN to reduce file size
        div_id='state-rankings-map',
        full_html=True  # Use Plotly's default HTML structure
    )
    
    # Add minimal CSS to make it responsive and transparent for iframe embedding
    # Insert style tag right after <head>
    style_insert = '''    <style>
        html, body {
            margin: 0;
            padding: 0;
            background: transparent;
            overflow: hidden;
            width: 100%;
            height: 100%;
        }
        #state-rankings-map {
            width: 100%;
            height: 100%;
            background: transparent;
        }
        .plotly-graph-div {
            background: transparent !important;
        }
    </style>
'''
    # Insert style after <head> tag
    full_html = full_html.replace('<head>', '<head>\n' + style_insert, 1)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_html)
    
    print(f"Interactive map saved to: {output_path}")
    
    # Also save as SVG
    svg_path = output_path.with_suffix('.svg')
    try:
        fig.write_image(str(svg_path), width=1200, height=700, scale=2)
        print(f"SVG map saved to: {svg_path}")
    except Exception as e:
        print(f"Warning: Could not save SVG (may need kaleido: pip install kaleido): {e}")
    
    return fig


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate interactive choropleth map of state ego rankings'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for HTML map (default: saves to main_outputs/mapping/)'
    )
    
    args = parser.parse_args()
    
    create_state_rankings_map(output_path=args.output)


if __name__ == "__main__":
    main()

