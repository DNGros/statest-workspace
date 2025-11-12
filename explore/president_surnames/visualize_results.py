"""Simple visualization and summary of president street name results."""

from pathlib import Path
import polars as pl

def main():
    # Load the results
    results_dir = Path(__file__).parent
    overall = pl.read_csv(results_dir / "president_streets_overall.csv")
    by_state = pl.read_csv(results_dir / "president_streets_by_state.csv")
    
    print("=" * 80)
    print("US PRESIDENT SURNAMES IN STREET NAMES - SUMMARY")
    print("=" * 80)
    
    # Overall statistics
    print("\n📊 OVERALL STATISTICS")
    print("-" * 80)
    total = overall["street_count"].sum()
    print(f"Total streets with president surnames: {total:,}")
    print(f"Number of presidents represented: {len(overall)}")
    print(f"Average streets per president: {total / len(overall):.0f}")
    
    # Top 10
    print("\n🏆 TOP 10 MOST COMMON PRESIDENT SURNAMES")
    print("-" * 80)
    for i, row in enumerate(overall.head(10).iter_rows(named=True), 1):
        pct = (row["street_count"] / total) * 100
        print(f"{i:2d}. {row['president_surname']:15s} {row['street_count']:6,} streets ({pct:5.2f}%)")
    
    # Bottom 10
    print("\n📉 BOTTOM 10 LEAST COMMON PRESIDENT SURNAMES")
    print("-" * 80)
    for i, row in enumerate(overall.tail(10).iter_rows(named=True), 1):
        pct = (row["street_count"] / total) * 100
        print(f"{i:2d}. {row['president_surname']:15s} {row['street_count']:6,} streets ({pct:5.2f}%)")
    
    # By state - top states for each president
    print("\n🗺️  TOP STATES BY PRESIDENT")
    print("-" * 80)
    
    # Show top 3 states for the top 5 presidents
    top_presidents = overall.head(5)["president_surname"].to_list()
    
    for president in top_presidents:
        president_states = (
            by_state
            .filter(pl.col("president_surname") == president)
            .sort("street_count", descending=True)
            .head(3)
        )
        
        print(f"\n{president}:")
        for row in president_states.iter_rows(named=True):
            state_name = row["state"].replace("-", " ").title()
            print(f"  • {state_name:20s} {row['street_count']:4,} streets")
    
    # States with most president streets overall
    print("\n🏛️  STATES WITH MOST PRESIDENT STREETS (Overall)")
    print("-" * 80)
    state_totals = (
        by_state
        .group_by("state")
        .agg(pl.col("street_count").sum().alias("total_streets"))
        .sort("total_streets", descending=True)
        .head(10)
    )
    
    for i, row in enumerate(state_totals.iter_rows(named=True), 1):
        state_name = row["state"].replace("-", " ").title()
        print(f"{i:2d}. {state_name:20s} {row['total_streets']:6,} streets")
    
    # Interesting facts
    print("\n💡 INTERESTING FACTS")
    print("-" * 80)
    
    # Recent presidents
    recent = overall.filter(
        pl.col("president_surname").is_in(["Obama", "Trump", "Biden"])
    ).sort("street_count", descending=True)
    
    print("\nRecent Presidents (Obama, Trump, Biden):")
    for row in recent.iter_rows(named=True):
        print(f"  • {row['president_surname']:10s} {row['street_count']:4,} streets")
    
    # Founding fathers
    founding = overall.filter(
        pl.col("president_surname").is_in(["Washington", "Jefferson", "Adams", "Madison"])
    ).sort("street_count", descending=True)
    
    print("\nFounding Fathers (Washington, Jefferson, Adams, Madison):")
    founding_total = founding["street_count"].sum()
    print(f"  Combined: {founding_total:,} streets ({(founding_total/total)*100:.1f}% of all president streets)")
    for row in founding.iter_rows(named=True):
        print(f"  • {row['president_surname']:10s} {row['street_count']:6,} streets")
    
    print("\n" + "=" * 80)
    print(f"Full results saved in: {results_dir}")
    print("  • president_streets_overall.csv - Counts by president")
    print("  • president_streets_by_state.csv - Counts by president and state")
    print("=" * 80)


if __name__ == "__main__":
    main()


